# Defining Daily Active Users (DAU) and Monthly Active Users (MAU)

## Overview
For a note-taking app, active user metrics are critical KPIs. Here are the most common definitions with their trade-offs:

---

## **Daily Active Users (DAU) Definitions**

### **Definition 1: Simple Presence (Most Common)**
**Criteria:** Any user who opens the app or accesses the service at least once during a calendar day (UTC or local timezone).

**Formula:**
```
DAU = COUNT(DISTINCT user_id) 
WHERE DATE(event_timestamp) = target_date 
AND event_type IN ('app_open', 'web_session_start', 'api_access')
```

**Pros:**
- Easy to implement and understand
- Works across web and mobile
- Directly measures reach

**Cons:**
- Includes inactive lurkers (open app then close)
- Bot activity can inflate numbers
- Doesn't measure actual engagement

---

### **Definition 2: Activity-Based (Stricter)**
**Criteria:** Users who perform a meaningful action (create, edit, or view note; not just login).

**Formula:**
```
DAU = COUNT(DISTINCT user_id) 
WHERE DATE(event_timestamp) = target_date 
AND event_type IN ('note_created', 'note_edited', 'note_viewed', 'note_shared')
AND duration_seconds > 0  -- exclude zero interactions
```

**Pros:**
- Reflects actual engagement
- Better for business metrics
- Reduces bot/spam noise

**Cons:**
- May miss power users who review without editing
- Harder to standardize across platforms
- Can miss legitimate use cases (e.g., read-only access)

---

### **Definition 3: Session-Based (Most Refined)**
**Criteria:** Users with at least one session ≥ 30 seconds (configurable threshold) during a calendar day.

**Formula:**
```
DAU = COUNT(DISTINCT user_id) 
WHERE DATE(session_start_date) = target_date 
AND session_duration_seconds >= 30
AND NOT is_bot
```

**Pros:**
- Filters out accidental opens
- Balances ease and engagement measurement
- Bot detection built-in

**Cons:**
- Requires session infrastructure
- Threshold is somewhat arbitrary
- May exclude power users with many micro-sessions

---

## **Monthly Active Users (MAU) Definitions**

### **Definition 1: Calendar Month (Simplest)**
**Criteria:** Any user with at least one qualifying event during a calendar month.

**Formula:**
```
MAU = COUNT(DISTINCT user_id) 
WHERE YEAR(event_timestamp) = target_year 
AND MONTH(event_timestamp) = target_month
AND event_type IN ('app_open', 'api_access', 'web_session_start')
```

**Pros:**
- Easy reporting (aligns with business calendars)
- Consistent with financial periods
- Simple to communicate

**Cons:**
- Includes users with 1 event on day 1 and none after
- Doesn't reflect retention
- Can be skewed by one-time users

---

### **Definition 2: Rolling 30-Day Window (Most Common Industry Standard)**
**Criteria:** Users with at least one qualifying event in the last 30 days (from report date).

**Formula:**
```
MAU = COUNT(DISTINCT user_id) 
WHERE event_timestamp >= (report_date - INTERVAL '30 days')
AND event_timestamp < report_date
AND event_type IN ('note_created', 'note_edited', 'note_viewed', 'api_access')
```

**Pros:**
- Better reflects active user base
- Removes seasonality bias
- Industry-standard metric
- Better for investor communications

**Cons:**
- More complex calculations (day-dependent)
- Can be confusing to non-technical stakeholders
- Requires careful date handling

---

### **Definition 3: Activity Threshold + Engagement (Strictest)**
**Criteria:** Users with ≥3 meaningful actions in the last 30 days AND at least 2 separate days of activity.

**Formula:**
```
MAU = COUNT(DISTINCT user_id) 
WHERE (
  SELECT COUNT(DISTINCT event_id) 
  FROM events 
  WHERE user_id = u.id 
  AND event_type IN ('note_created', 'note_edited', 'note_shared')
  AND timestamp >= (report_date - INTERVAL '30 days')
) >= 3
AND (
  SELECT COUNT(DISTINCT DATE(timestamp))
  FROM events 
  WHERE user_id = u.id 
  AND timestamp >= (report_date - INTERVAL '30 days')
) >= 2
```

**Pros:**
- True measure of engaged users
- Separates trial/churned users
- Better for retention analysis

**Cons:**
- Complex to explain and maintain
- May undercount legitimate users
- Requires careful event tracking

---

## **Recommended Approach for Note-Taking App**

| Metric | Recommended Definition | Rationale |
|--------|------------------------|-----------|
| **DAU** | Definition 2 (Activity-Based) | Note-taking requires actual content engagement |
| **MAU** | Definition 2 (Rolling 30-Day) | Industry standard, better growth metric |
| **Engaged MAU** | Definition 3 | Separate metric for retention/quality analysis |

---

## **Key Considerations**

1. **Bot/Spam Filtering:** Implement before counting (check: IP reputation, user agent, behavior patterns)
2. **Timezone Selection:** Use UTC for consistency, but segment by user's local timezone for accuracy
3. **Segmentation:** Always break down by:
   - Platform (web/iOS/Android)
   - Account type (free/premium)
   - Geography
4. **Retention Funnel:** DAU/MAU ratio is key → DAU/MAU Ratio = (DAU / MAU) × 100%
   - Healthy apps: 15-25% ratio
   - Highly engaged: 30%+
5. **Cohort Analysis:** Track DAU/MAU by cohort (sign-up date) to spot trends
6. **Data Quality:** Monitor for:
   - Data pipeline latency (24-48 hour delay acceptable)
   - Duplicate events
   - Missing events from certain platforms

---

This framework gives you multiple options to align metrics with your business goals while maintaining analytical rigor.
