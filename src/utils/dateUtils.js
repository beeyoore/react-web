/**
 * Calculate days elapsed since a given date
 * @param {string} dateString - ISO date string (YYYY-MM-DD)
 * @returns {number} Number of days elapsed
 */
export function getDaysSinceDate(dateString) {
  if (!dateString) return 0;
  
  try {
    const startDate = new Date(dateString);
    const today = new Date();
    
    // Reset time to midnight for accurate day calculation
    startDate.setHours(0, 0, 0, 0);
    today.setHours(0, 0, 0, 0);
    
    const diffMs = today - startDate;
    const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24));
    
    return Math.max(0, diffDays);
  } catch (error) {
    console.error('Error calculating days since date:', error);
    return 0;
  }
}

/**
 * Determine which homepage variant to show based on days since first login
 * @param {number} daysSinceFirstLogin - Number of days elapsed
 * @returns {string} 'T0' or 'T1'
 */
export function getHomepageVariant(daysSinceFirstLogin) {
  return daysSinceFirstLogin >= 15 ? 'T1' : 'T0';
}
