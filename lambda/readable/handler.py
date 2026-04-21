import json
import logging
import os
import time
import urllib.parse
from collections import Counter

import boto3
from botocore.exceptions import ClientError


s3 = boto3.client("s3")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

ALLOWED_TIPI = {"controlli", "stipendi"}


def build_practice_output_prefix(prefix, tipo_servizio=None, id_pratica=None):
    normalized_prefix = prefix.strip("/")
    parts = ["output", normalized_prefix]
    if tipo_servizio:
        parts.append(tipo_servizio)
    if id_pratica:
        parts.append(id_pratica)
    return "/".join(parts)


def build_legacy_practice_output_prefix(prefix, id_pratica=None):
    normalized_prefix = prefix.strip("/")
    if id_pratica:
        return f"pratiche/{id_pratica}/output/{normalized_prefix}"
    return normalized_prefix


def parse_raw_key(key, raw_prefix):
    parts = key.split("/")

    # flat: {raw_prefix}/{source_file}/{job_id}/{part}
    if len(parts) == 4 and parts[0] == raw_prefix:
        part_name = parts[3]
        if not part_name.isdigit():
            return None

        return {
            "source_file": parts[1],
            "job_id": parts[2],
            "tipo_servizio": None,
            "id_pratica": None,
            "raw_prefix": raw_prefix,
            "path_style": "flat",
        }

    # legacy_practice: pratiche/{id_pratica}/output/{raw_prefix}/{source_file}/{job_id}/{part}
    if (
        len(parts) == 7
        and parts[0] == "pratiche"
        and parts[2] == "output"
        and parts[3] == raw_prefix
        and parts[6].isdigit()
    ):
        return {
            "source_file": parts[4],
            "job_id": parts[5],
            "tipo_servizio": None,
            "id_pratica": parts[1],
            "raw_prefix": "/".join(parts[:4]),
            "path_style": "legacy_practice",
        }

    # phase_first: output/{raw_prefix}/{id_pratica}/{source_file}/{job_id}/{part}
    if (
        len(parts) == 6
        and parts[0] == "output"
        and parts[1] == raw_prefix
        and parts[5].isdigit()
    ):
        return {
            "source_file": parts[3],
            "job_id": parts[4],
            "tipo_servizio": None,
            "id_pratica": parts[2],
            "raw_prefix": "/".join(parts[:3]),
            "path_style": "phase_first",
        }

    # practice_with_tipo: output/{raw_prefix}/{tipo_servizio}/{id_pratica}/{source_file}/{job_id}/{part}
    if (
        len(parts) == 7
        and parts[0] == "output"
        and parts[1] == raw_prefix
        and parts[2] in ALLOWED_TIPI
        and parts[6].isdigit()
    ):
        return {
            "source_file": parts[4],
            "job_id": parts[5],
            "tipo_servizio": parts[2],
            "id_pratica": parts[3],
            "raw_prefix": "/".join(parts[:4]),
            "path_style": "practice_with_tipo",
        }

    return None


def list_raw_part_keys(bucket, raw_prefix, source_file, job_id):
    prefix = f"{raw_prefix}/{source_file}/{job_id}/"
    continuation_token = None
    keys = []

    while True:
        params = {"Bucket": bucket, "Prefix": prefix}
        if continuation_token:
            params["ContinuationToken"] = continuation_token

        response = s3.list_objects_v2(**params)
        contents = response.get("Contents", [])
        keys.extend(
            item["Key"]
            for item in contents
            if item["Key"].rsplit("/", 1)[-1].isdigit()
        )

        if not response.get("IsTruncated"):
            break

        continuation_token = response.get("NextContinuationToken")

    return sorted(keys, key=lambda key: int(key.rsplit("/", 1)[-1]))


def load_raw_parts(bucket, part_keys):
    blocks = []
    document_pages = 0
    status = None
    model_version = None

    for key in part_keys:
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        data = json.loads(body)
        blocks.extend(data.get("Blocks", []))
        document_pages = max(document_pages, data.get("DocumentMetadata", {}).get("Pages", 0))
        status = data.get("JobStatus", status)
        model_version = data.get("AnalyzeDocumentModelVersion", model_version)

    return {
        "blocks": blocks,
        "document_pages": document_pages,
        "status": status,
        "model_version": model_version,
    }


def load_json_object(bucket, key):
    try:
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        if error_code in {"404", "NoSuchKey", "NotFound"}:
            return None
        raise

    return json.loads(body)


def get_pages_present(blocks):
    return sorted({block.get("Page") for block in blocks if block.get("Page")})


def load_complete_raw_parts(bucket, raw_prefix, source_file, job_id, max_attempts=6, sleep_seconds=2):
    latest_part_keys = []
    latest_raw_parts = None
    latest_pages_present = []
    previous_part_keys = None

    for attempt in range(1, max_attempts + 1):
        latest_part_keys = list_raw_part_keys(bucket, raw_prefix, source_file, job_id)
        if not latest_part_keys:
            return [], None, []

        latest_raw_parts = load_raw_parts(bucket, latest_part_keys)
        latest_pages_present = get_pages_present(latest_raw_parts["blocks"])
        has_all_pages = (
            latest_raw_parts["document_pages"] == 0
            or len(latest_pages_present) >= latest_raw_parts["document_pages"]
        )
        parts_are_stable = previous_part_keys == latest_part_keys
        is_complete = has_all_pages and parts_are_stable

        logger.info(
            "readable_completeness_check source_file=%s job_id=%s attempt=%s part_count=%s pages_present=%s document_pages=%s has_all_pages=%s parts_are_stable=%s is_complete=%s",
            source_file,
            job_id,
            attempt,
            len(latest_part_keys),
            latest_pages_present,
            latest_raw_parts["document_pages"],
            has_all_pages,
            parts_are_stable,
            is_complete,
        )

        if is_complete:
            return latest_part_keys, latest_raw_parts, latest_pages_present

        previous_part_keys = list(latest_part_keys)
        if attempt < max_attempts:
            time.sleep(sleep_seconds)

    return latest_part_keys, latest_raw_parts, latest_pages_present


def sort_by_position(block):
    box = block.get("Geometry", {}).get("BoundingBox", {})
    return (
        block.get("Page", 0),
        box.get("Top", 0),
        box.get("Left", 0),
    )


def build_block_map(blocks):
    return {block["Id"]: block for block in blocks if "Id" in block}


def get_relationship_ids(block, relationship_type):
    ids = []
    for relationship in block.get("Relationships") or []:
        if relationship.get("Type") == relationship_type:
            ids.extend(relationship.get("Ids", []))
    return ids


def get_geometry(block):
    geometry = block.get("Geometry") or {}
    return {
        "boundingBox": geometry.get("BoundingBox"),
        "polygon": geometry.get("Polygon"),
    }


def extract_content(block, block_map):
    words = []
    selections = []

    for child_id in get_relationship_ids(block, "CHILD"):
        child = block_map.get(child_id)
        if not child:
            continue

        block_type = child.get("BlockType")
        if block_type == "WORD" and child.get("Text"):
            words.append(child["Text"])
        elif block_type == "SELECTION_ELEMENT":
            selection = {
                "status": child.get("SelectionStatus"),
                "confidence": child.get("Confidence"),
                "geometry": get_geometry(child),
            }
            selections.append(selection)
            if child.get("SelectionStatus") == "SELECTED":
                words.append("X")

    return {
        "text": " ".join(words).strip(),
        "selectionElements": selections,
    }


def collect_pages_text(blocks):
    lines = [block for block in blocks if block.get("BlockType") == "LINE" and block.get("Text")]
    lines.sort(key=sort_by_position)

    pages = {}
    for block in lines:
        pages.setdefault(block.get("Page", 1), []).append(block["Text"])

    pages_text = []
    for page_number in sorted(pages):
        text = "\n".join(pages[page_number])
        pages_text.append({
            "page": page_number,
            "text": text,
        })

    full_text = "\n\n".join(page["text"] for page in pages_text if page["text"])
    return pages_text, full_text


def collect_forms(blocks, block_map):
    forms = []
    key_blocks = [
        block for block in blocks
        if block.get("BlockType") == "KEY_VALUE_SET" and "KEY" in (block.get("EntityTypes") or [])
    ]
    key_blocks.sort(key=sort_by_position)

    for key_block in key_blocks:
        key_content = extract_content(key_block, block_map)
        if not key_content["text"] and not key_content["selectionElements"]:
            continue

        value_data = {
            "id": None,
            "text": "",
            "confidence": None,
            "geometry": None,
            "selectionElements": [],
        }

        for value_id in get_relationship_ids(key_block, "VALUE"):
            value_block = block_map.get(value_id)
            if not value_block:
                continue

            value_content = extract_content(value_block, block_map)
            value_data = {
                "id": value_block.get("Id"),
                "text": value_content["text"],
                "confidence": value_block.get("Confidence"),
                "geometry": get_geometry(value_block),
                "selectionElements": value_content["selectionElements"],
            }
            break

        forms.append(
            {
                "page": key_block.get("Page", 1),
                "key": {
                    "id": key_block.get("Id"),
                    "text": key_content["text"],
                    "confidence": key_block.get("Confidence"),
                    "geometry": get_geometry(key_block),
                },
                "value": value_data,
            }
        )

    return forms


def collect_tables(blocks, block_map):
    tables = []
    table_blocks = [block for block in blocks if block.get("BlockType") == "TABLE"]
    table_blocks.sort(key=sort_by_position)

    for index, table_block in enumerate(table_blocks, start=1):
        rows = {}
        max_column = 0

        for cell_id in get_relationship_ids(table_block, "CHILD"):
            cell = block_map.get(cell_id)
            if not cell or cell.get("BlockType") != "CELL":
                continue

            content = extract_content(cell, block_map)
            row_index = cell.get("RowIndex", 1)
            column_index = cell.get("ColumnIndex", 1)
            max_column = max(max_column, column_index)
            rows.setdefault(row_index, {})[column_index] = content["text"]

        normalized_rows = []
        for row_index in sorted(rows):
            row = rows[row_index]
            normalized_rows.append([row.get(column, "") for column in range(1, max_column + 1)])

        tables.append(
            {
                "page": table_block.get("Page", 1),
                "tableIndex": index,
                "rowCount": len(normalized_rows),
                "columnCount": max_column,
                "rows": normalized_rows,
            }
        )

    return tables


def build_clean_forms(forms):
    clean_forms = []

    for form in forms:
        key_text = ((form.get("key") or {}).get("text") or "").strip()
        value_text = ((form.get("value") or {}).get("text") or "").strip()

        if not key_text and not value_text:
            continue

        clean_forms.append(
            {
                "page": form.get("page"),
                "key": {"text": key_text},
                "value": {"text": value_text},
            }
        )

    return clean_forms


def build_clean_tables(tables):
    clean_tables = []

    for table in tables:
        clean_tables.append(
            {
                "page": table.get("page"),
                "tableIndex": table.get("tableIndex"),
                "rowCount": table.get("rowCount"),
                "columnCount": table.get("columnCount"),
                "rows": table.get("rows", []),
            }
        )

    return clean_tables


def group_forms_by_page(forms):
    grouped = []
    by_page = {}

    for form in forms:
        page = form.get("page", 1)
        by_page.setdefault(page, []).append(
            {
                "key": form.get("key", {}),
                "value": form.get("value", {}),
            }
        )

    for page in sorted(by_page):
        grouped.append(
            {
                "page": page,
                "items": by_page[page],
            }
        )

    return grouped


def group_tables_by_page(tables):
    grouped = []
    by_page = {}

    for table in tables:
        page = table.get("page", 1)
        by_page.setdefault(page, []).append(
            {
                "tableIndex": table.get("tableIndex"),
                "rowCount": table.get("rowCount"),
                "columnCount": table.get("columnCount"),
                "rows": table.get("rows", []),
            }
        )

    for page in sorted(by_page):
        grouped.append(
            {
                "page": page,
                "items": by_page[page],
            }
        )

    return grouped


def build_document(blocks, document_pages, status, model_version, source_file, job_id):
    block_map = build_block_map(blocks)
    block_counts = dict(Counter(block.get("BlockType") for block in blocks))
    pages_present = get_pages_present(blocks)
    pages_text, full_text = collect_pages_text(blocks)
    forms = build_clean_forms(collect_forms(blocks, block_map))
    tables = build_clean_tables(collect_tables(blocks, block_map))

    return {
        "jobId": job_id,
        "sourceFile": source_file,
        "metadata": {
            "status": status,
            "modelVersion": model_version,
            "documentPages": document_pages,
            "pagesPresent": pages_present,
        },
        "summary": {
            "lineCount": block_counts.get("LINE", 0),
            "wordCount": block_counts.get("WORD", 0),
            "formCount": len(forms),
            "tableCount": len(tables),
        },
        "textPreview": full_text[:6000],
        "pagesText": pages_text,
        "formsByPage": group_forms_by_page(forms),
        "tablesByPage": group_tables_by_page(tables),
        "text": full_text,
    }


def build_consolidated_raw_document(raw_parts, source_file, job_id, part_keys, pages_present):
    return {
        "jobId": job_id,
        "sourceFile": source_file,
        "sourcePartKeys": part_keys,
        "metadata": {
            "status": raw_parts["status"],
            "modelVersion": raw_parts["model_version"],
            "documentPages": raw_parts["document_pages"],
            "pagesPresent": pages_present,
            "partCount": len(part_keys),
        },
        "blocks": raw_parts["blocks"],
    }


def build_context_key(document_context_prefix, source_file, tipo_servizio=None, id_pratica=None):
    scoped_prefix = build_practice_output_prefix(document_context_prefix, tipo_servizio, id_pratica)
    return f"{scoped_prefix}/{source_file}.json"


def build_legacy_context_key(document_context_prefix, source_file, id_pratica=None):
    scoped_prefix = build_legacy_practice_output_prefix(document_context_prefix, id_pratica)
    return f"{scoped_prefix}/{source_file}.json"


def build_consolidated_raw_key(raw_consolidated_prefix, source_file, tipo_servizio=None, id_pratica=None):
    scoped_prefix = build_practice_output_prefix(raw_consolidated_prefix, tipo_servizio, id_pratica)
    return f"{scoped_prefix}/{source_file}.json"


def build_output_key(clean_prefix, source_file, tipo_servizio=None, id_pratica=None):
    scoped_prefix = build_practice_output_prefix(clean_prefix, tipo_servizio, id_pratica)
    return f"{scoped_prefix}/{source_file}.json"


def apply_document_context(document, document_context, context_key):
    if not document_context:
        return document

    enriched_document = dict(document)
    metadata = dict(enriched_document.get("metadata", {}))

    id_pratica = document_context.get("id_pratica")
    if id_pratica:
        enriched_document["id_pratica"] = id_pratica
        metadata["id_pratica"] = id_pratica

    tipo_servizio = document_context.get("tipo_servizio")
    if tipo_servizio:
        enriched_document["tipo_servizio"] = tipo_servizio
        metadata["tipo_servizio"] = tipo_servizio

    source_bucket = document_context.get("sourceBucket")
    source_key = document_context.get("sourceKey")
    if source_bucket:
        enriched_document["sourceInputBucket"] = source_bucket
    if source_key:
        enriched_document["sourceInputKey"] = source_key

    enriched_document["sourceContextKey"] = context_key
    enriched_document["metadata"] = metadata
    return enriched_document


def lambda_handler(event, context):
    logger.info("readable_event_received event=%s", json.dumps(event))

    raw_prefix = os.environ.get("RAW_PREFIX", "raw").strip("/")
    raw_consolidated_prefix = os.environ.get("RAW_CONSOLIDATED_PREFIX", "raw_consolidated").strip("/")
    clean_prefix = os.environ.get("CLEAN_PREFIX", "clean").strip("/")
    document_context_prefix = os.environ.get("DOCUMENT_CONTEXT_PREFIX", "document_context").strip("/")
    saved_files = []

    for record in event.get("Records", []):
        if record.get("eventSource") != "aws:s3":
            logger.info("readable_skip reason=non_s3_record")
            continue

        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
        logger.info("readable_event bucket=%s key=%s", bucket, key)
        parsed = parse_raw_key(key, raw_prefix)
        if not parsed:
            logger.info("readable_skip key=%s reason=invalid_raw_key", key)
            continue

        part_keys, raw_parts, pages_present = load_complete_raw_parts(
            bucket,
            parsed["raw_prefix"],
            parsed["source_file"],
            parsed["job_id"],
        )
        if not part_keys or not raw_parts:
            logger.info(
                "readable_skip source_file=%s job_id=%s reason=no_part_keys",
                parsed["source_file"],
                parsed["job_id"],
            )
            continue

        if raw_parts["document_pages"] and len(pages_present) < raw_parts["document_pages"]:
            logger.warning(
                "readable_skip source_file=%s job_id=%s reason=incomplete_raw_output pages_present=%s document_pages=%s",
                parsed["source_file"],
                parsed["job_id"],
                len(pages_present),
                raw_parts["document_pages"],
            )
            continue

        logger.info(
            "readable_raw_loaded source_file=%s job_id=%s blocks=%s pages=%s status=%s",
            parsed["source_file"],
            parsed["job_id"],
            len(raw_parts["blocks"]),
            raw_parts["document_pages"],
            raw_parts["status"],
        )

        tipo_servizio = parsed.get("tipo_servizio")
        id_pratica = parsed.get("id_pratica")

        context_key = build_context_key(document_context_prefix, parsed["source_file"], tipo_servizio, id_pratica)
        document_context = load_json_object(bucket, context_key)
        if not document_context and parsed.get("path_style") == "legacy_practice":
            legacy_context_key = build_legacy_context_key(document_context_prefix, parsed["source_file"], id_pratica)
            legacy_document_context = load_json_object(bucket, legacy_context_key)
            if legacy_document_context:
                context_key = legacy_context_key
                document_context = legacy_document_context

        # Integra tipo_servizio dal document_context se non ricavabile dal path
        if document_context and not tipo_servizio:
            tipo_servizio = document_context.get("tipo_servizio")

        if document_context:
            logger.info(
                "readable_context_loaded source_file=%s context_key=%s tipo_servizio=%s id_pratica=%s",
                parsed["source_file"],
                context_key,
                tipo_servizio,
                document_context.get("id_pratica"),
            )
        else:
            logger.warning(
                "readable_context_missing source_file=%s context_key=%s",
                parsed["source_file"],
                context_key,
            )

        consolidated_raw_document = build_consolidated_raw_document(
            raw_parts,
            parsed["source_file"],
            parsed["job_id"],
            part_keys,
            pages_present,
        )
        consolidated_raw_document = apply_document_context(consolidated_raw_document, document_context, context_key)
        consolidated_raw_key = build_consolidated_raw_key(raw_consolidated_prefix, parsed["source_file"], tipo_servizio, id_pratica)
        existing_raw_document = load_json_object(bucket, consolidated_raw_key)
        if not existing_raw_document or existing_raw_document.get("jobId") != parsed["job_id"]:
            logger.info(
                "readable_write_consolidated_raw_start output_key=%s part_count=%s",
                consolidated_raw_key,
                len(part_keys),
            )
            s3.put_object(
                Bucket=bucket,
                Key=consolidated_raw_key,
                Body=json.dumps(consolidated_raw_document, ensure_ascii=False, indent=2).encode("utf-8"),
                ContentType="application/json",
            )
            logger.info("readable_write_consolidated_raw_done output_key=%s", consolidated_raw_key)
        else:
            logger.info(
                "readable_skip output_key=%s reason=consolidated_raw_already_current job_id=%s",
                consolidated_raw_key,
                parsed["job_id"],
            )

        cleaned_document = build_document(
            consolidated_raw_document["blocks"],
            consolidated_raw_document["metadata"]["documentPages"],
            consolidated_raw_document["metadata"]["status"],
            consolidated_raw_document["metadata"]["modelVersion"],
            parsed["source_file"],
            parsed["job_id"],
        )
        cleaned_document = apply_document_context(cleaned_document, document_context, context_key)
        cleaned_document["sourceRawConsolidatedKey"] = consolidated_raw_key

        output_key = build_output_key(clean_prefix, parsed["source_file"], tipo_servizio, id_pratica)
        existing_document = load_json_object(bucket, output_key)
        if existing_document and existing_document.get("jobId") == parsed["job_id"]:
            logger.info(
                "readable_skip output_key=%s reason=clean_already_current job_id=%s",
                output_key,
                parsed["job_id"],
            )
            continue

        if existing_document:
            logger.info(
                "readable_overwrite output_key=%s previous_job_id=%s new_job_id=%s",
                output_key,
                existing_document.get("jobId"),
                parsed["job_id"],
            )

        logger.info("readable_write_output_start output_key=%s source_raw_key=%s", output_key, consolidated_raw_key)
        s3.put_object(
            Bucket=bucket,
            Key=output_key,
            Body=json.dumps(cleaned_document, ensure_ascii=False, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
        logger.info("readable_write_output_done output_key=%s", output_key)

        saved_files.append({
            "sourceKey": key,
            "contextKey": context_key,
            "consolidatedRawKey": consolidated_raw_key,
            "outputKey": output_key,
            "jobId": parsed["job_id"],
            "tipoServizio": tipo_servizio,
            "idPratica": (document_context or {}).get("id_pratica"),
        })

    logger.info("readable_completed saved_files=%s", json.dumps(saved_files, ensure_ascii=False))
    return {"savedFiles": saved_files}