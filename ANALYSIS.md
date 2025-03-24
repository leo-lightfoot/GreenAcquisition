# Event Study Analysis: Green Acquisitions in the Energy Sector

## Overview
This document presents a detailed analysis of the event study results examining the stock market reaction to M&A announcements in the energy sector, with a particular focus on green vs. brown company classifications. The analysis covers both 3-day and 10-day event windows around the announcement dates.

## Sample Characteristics
- Total sample size: 827 M&A deals
- Green target acquisitions: 191 deals (23.1% of total)
- Brown acquirer - Green target deals: 137 deals (16.6% of total)

## Key Findings

### 1. Overall Market Reaction (All Deals)

#### 3-Day Window
- Mean Abnormal Return: +0.097%
- Median Abnormal Return: +0.067%
- Standard Deviation: 6.69%
- Market Cap-Weighted Return: -0.61%
- Statistical Significance: Not significant (p-value = 0.677)

#### 10-Day Window
- Mean Abnormal Return: +0.342%
- Median Abnormal Return: +0.193%
- Standard Deviation: 9.87%
- Market Cap-Weighted Return: -0.434%
- Statistical Significance: Not significant (p-value = 0.319)

**Interpretation**: The overall market reaction to M&A announcements in the energy sector is slightly positive but not statistically significant in both windows. The higher standard deviation in the 10-day window suggests increased price volatility over longer periods.

### 2. Green Target Acquisitions

#### 3-Day Window
- Mean Abnormal Return: +0.726%
- Median Abnormal Return: +0.252%
- Standard Deviation: 6.10%
- Market Cap-Weighted Return: +0.547%
- Statistical Significance: Marginally significant (p-value = 0.101)

#### 10-Day Window
- Mean Abnormal Return: +0.581%
- Median Abnormal Return: +0.231%
- Standard Deviation: 9.84%
- Market Cap-Weighted Return: +1.391%
- Statistical Significance: Not significant (p-value = 0.415)

**Interpretation**: Acquisitions of green targets show more positive returns, particularly in the short term (3-day window). The market cap-weighted returns are notably positive, suggesting larger deals perform better.

### 3. Brown Acquirer - Green Target Deals

#### 3-Day Window
- Mean Abnormal Return: +0.811%
- Median Abnormal Return: +0.088%
- Standard Deviation: 6.25%
- Market Cap-Weighted Return: +0.814%
- Statistical Significance: Marginally significant (p-value = 0.131)
- Carbon Intensity Correlation: Significant negative correlation (p-value = 0.039)

#### 10-Day Window
- Mean Abnormal Return: +0.783%
- Median Abnormal Return: +0.954%
- Standard Deviation: 9.32%
- Market Cap-Weighted Return: +2.245%
- Statistical Significance: Not significant (p-value = 0.327)
- Carbon Intensity Correlation: Negative but not significant (p-value = 0.160)

**Interpretation**: Brown companies acquiring green targets show the strongest positive returns, especially in market cap-weighted terms. The negative correlation with carbon intensity suggests that among brown acquirers, those with lower carbon intensity see better market reactions.

## Carbon Intensity Analysis

### Relationship with Returns
1. **Short-term (3-day) Effects**:
   - Significant negative correlation for brown acquirer - green target deals
   - Coefficient: -0.617 (p-value = 0.039)
   - R-squared: 3.10%

2. **Longer-term (10-day) Effects**:
   - Similar negative correlation but weaker statistical significance
   - Coefficient: -0.632 (p-value = 0.160)
   - R-squared: 1.46%

### Key Implications
1. The market appears to reward brown companies that acquire green targets, particularly those with lower carbon intensity.
2. The effect is stronger and more statistically significant in the short term (3-day window).
3. Market cap-weighted returns suggest larger deals perform better, especially in the longer window.

## Temporal Pattern Analysis

1. **Short-term vs. Long-term Effects**:
   - Immediate reaction (3-day) shows stronger statistical significance
   - Longer window (10-day) shows higher absolute returns but with more noise
   - Standard deviation increases by roughly 50% in the 10-day window

2. **Return Persistence**:
   - Green target acquisitions show positive returns in both windows
   - Brown acquirer - green target deals maintain positive returns across both periods
   - Market cap-weighted returns improve in the longer window for green-related deals

## Conclusions

1. **Market Perception**: The market generally views green acquisitions positively, with stronger effects for brown companies acquiring green targets.

2. **Size Effect**: Larger deals (as measured by market cap-weighted returns) tend to perform better, especially in green-related acquisitions.

3. **Carbon Intensity Impact**: Lower carbon intensity is associated with better market reaction, particularly in the short term.

4. **Timing Considerations**: 
   - Stronger statistical significance in the 3-day window suggests efficient price discovery
   - Higher absolute returns but more noise in the 10-day window indicates potential confounding factors

5. **Strategic Implications**:
   - Brown companies can benefit from green acquisitions
   - Lower carbon intensity companies are better positioned for successful green acquisitions
   - Larger deals tend to be received more positively by the market

## Recommendations for Future Research

1. Investigate the long-term performance beyond the 10-day window
2. Analyze the impact of deal size more explicitly
3. Consider the role of market conditions and energy sector cycles
4. Examine the impact of regulatory announcements and policy changes
5. Study the effect of green premium in valuation multiples 