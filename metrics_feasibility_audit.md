# Healthcare Metrics Project — Metrics Feasibility Audit

Assessed against the actual 19-file NH data catalog set (PBJ master + 18 CMS
supporting files, confirmed from your Google Drive folder). Per the project
brief's own disclaimer, not every listed metric is solvable — this document
identifies which ones are, and states plainly why the others aren't, rather
than silently skipping them.

**Legend:** ✅ Calculable  ⚠️ Calculable only as a proxy/redefinition  ❌ Not calculable

---

## Selected metrics for this submission (5)

These are fully calculable, minimally caveated, and each maps directly to a
line item in the brief — not invented substitutes. All 5 are implemented in
`streamlit_app.py`.

| # | Metric | Formula | Source |
|---|---|---|---|
| 1 | Total nursing hours worked, by hospital/state/month | `sum(Hrs_RN + Hrs_LPN + Hrs_CNA)` grouped by facility/state/`month(WorkDate)` | PBJ master |
| 2 | Hospital occupancy rate trend, by month | `MDScensus / Number of Certified Beds`, trended over `WorkDate` | PBJ + NH_ProviderInfo |
| 3 | Staffing levels vs. bed occupancy (comparison) | Scatter: `hours per resident per day` vs. `occupancy rate`, per facility | PBJ + NH_ProviderInfo |
| 4 | Correlation: nurse staffing vs. readmission rate | Correlation of facility-avg HPPD against `S_004_01_PPR_PD_RSRR` score | PBJ + NH_ProviderInfo + Skilled_Nursing_Facility_QRP_Provider_Data |
| 5 | Ratio of permanent (employed) to contract staff | `sum(*_ctr hours) / sum(*_emp hours)`, by facility/state | PBJ master |

**Caveat on #2 and any "trend"**: the master file is named
`PBJ_Daily_Nurse_Staffing_Q2_2024.csv` — a single quarter, not a full year.
Any monthly trend covers at most 3 months, not the "past year" the brief's
example describes. State this explicitly rather than implying more history
than exists.

---

## Full audit, by category

### 1. Staffing Metrics

| Metric | Verdict | Notes |
|---|---|---|
| Nurse-to-patient ratio by hospital, state, **and department** | ⚠️ | Hospital and state: ✅ calculable (`(Hrs_RN+Hrs_LPN+Hrs_CNA)/MDScensus`). **Department: ❌ impossible** — PBJ has no unit/ward/department field at all; it's a single facility-wide daily figure. |
| Total hours worked by nurses, by hospital/state/month | ✅ | Selected metric #1 above. |
| Percentage of nurses working overtime | ❌ | PBJ reports facility-day aggregated hours, not per-employee. There is no way to know if 200 hours came from 20 nurses working 10 hours or 5 nurses working 40 — overtime is fundamentally an individual-employee concept this data can't see. |
| Shifts per nurse (avg/median), by hospital/state | ❌ | No per-nurse identifier or shift-count field exists anywhere in the 19 files. |

### 2. Facility Metrics

| Metric | Verdict | Notes |
|---|---|---|
| Occupancy rate trend (monthly/quarterly, past year) | ✅* | Selected metric #2. *Only ~3 months of data available, not a year — see caveat above. |
| Bed utilization by hospital **and department** | ⚠️ | Hospital: ✅ (same calculation as occupancy rate). Department: ❌ (no department field). |
| Staffing levels vs. bed occupancy | ✅ | Selected metric #3. |
| Top 10 hospitals by patient throughput | ⚠️ | "Throughput" properly means admissions/discharges over time — not present. Best available proxy: rank facilities by average daily census, which is a *snapshot volume* proxy, not true throughput. Label accordingly if included. |
| Facilities with lowest staffing vs. patient load | ✅ | Trivial extension of metric #1/#3's underlying query, sorted ascending instead of descending. |

### 3. Quality Metrics

| Metric | Verdict | Notes |
|---|---|---|
| Patient satisfaction scores by hospital | ❌ | No satisfaction/survey field in any of the 19 files. |
| Average length of stay (ALOS), by department and state | ❌ | No length-of-stay or admission/discharge date field exists anywhere in this file set. |
| Readmission rate within 30 days, by hospital/state/**diagnosis category** | ⚠️ | Hospital and state: ✅ via `S_004_01_PPR_PD_RSRR` in the QRP provider file (joined to NH_ProviderInfo for state). Diagnosis category: ❌ — no diagnosis/condition breakdown exists; the QRP measure is a single facility-level aggregate rate. |
| Patient-to-nurse complaint ratio | ⚠️ | NH_ProviderInfo has "Number of Substantiated Complaints" — but this is a *general facility* complaint count, not nurse-specific, and not phrased as a per-patient ratio in the source. A proxy (`complaints / avg census`) can be built but should be labeled as approximate, not a true nurse-complaint measure. |
| Correlation: staffing vs. readmission rate | ✅ | Selected metric #4. |

### 4. Cost Metrics

| Metric | Verdict | Notes |
|---|---|---|
| Total payroll costs, by hospital/state | ❌ | No wage or dollar-cost data anywhere — PBJ has hours only. |
| Average cost per patient stay, by hospital/state | ❌ | No billing/cost data in any file. |
| Overtime cost as % of total payroll | ❌ | Depends on both overtime (impossible, see above) and payroll cost (also impossible). |
| Hospital revenue vs. payroll expenses | ❌ | No revenue data anywhere in this file set. |

**Every metric in this category is unanswerable with the current file set.**
The only dollar figures present anywhere are `Fine Amount in Dollars` (NH_Penalties) — regulatory fines, not payroll or revenue. If cost metrics are a hard requirement, they need a different data source entirely (a payroll or claims-revenue extract), not something derivable from PBJ or the CMS Nursing Home Compare catalog.

### 5. Operational Metrics

| Metric | Verdict | Notes |
|---|---|---|
| Shift utilization by time of day (morning/afternoon/night) | ❌ | PBJ is daily-aggregated with no intraday timestamps — there's no way to know when during the day hours were worked. |
| Peak staffing hours, by hospital **and department** | ⚠️ | Redefinable as "peak staffing *day*" (day-of-week or date with highest total hours) rather than peak *time of day* (impossible). Department dimension still unavailable. |
| Ratio of permanent to contract staff | ✅ | Selected metric #5. |
| Nurse attrition rate trend | ⚠️ | The turnover figures themselves ARE available (`Total nursing staff turnover`, `Registered Nurse turnover` in NH_ProviderInfo) — but this is a single dated snapshot (Oct 2024), not a time series. A genuine **trend** requires multiple monthly/quarterly ProviderInfo extracts over time, which aren't part of this file set. The current value can be reported; a trend cannot. |

---

## Bottom line

- **5 of 22** requested metrics are cleanly calculable and form the submission set above.
- **6 more** are calculable only as clearly-labeled proxies or with a dimension removed (department, diagnosis category, time-of-day — none of which exist anywhere in this file set).
- **11 of 22** — including the entire Cost Metrics category, all department-level breakdowns, patient satisfaction, ALOS, and true overtime — are not answerable with this data at all, regardless of query complexity. This isn't a modeling gap; the underlying facts were never collected in these files.
