# Initial data profile

Read-only inspection of all 154 downloaded files. No raw records were changed. Findings describe this snapshot, not future batches.

## Structure and grain

| Source | Format | Grain | Observed rows |
| --- | --- | --- | ---: |
| Participants | CSV | Participant | 50 |
| Device metadata | CSV | Device / participant | 50 |
| Wellness surveys | CSV | Participant and date | 1,414 |
| Steps | 50 JSON arrays | Participant and timestamp | 2,014,950 |
| Heart rate | 50 JSON arrays | Participant and timestamp | 2,014,950 |
| Sleep | 50 JSON arrays with nested stages | Participant and wake-up date | 1,400 |

The expected minute grid is 50 participants x 28 days x 1,440 minutes = 2,016,000 records per minute source. A near-correct total does not prove complete coverage.

## Findings

| Finding | Evidence | Proposed handling; not yet implemented |
| --- | --- | --- |
| Survey referential integrity | 14 rows reference P999, absent from participants. All 1,400 expected participant-days exist; scores are in range. | Quarantine unknown participant rows. |
| Missing firmware | 15 of 50 device records have blank firmware_version; every label is regex-parseable, and populated versions agree with labels. | Recover from device_label with provenance. |
| Mixed step units | P005, P033 and P038 have 120,963 rows explicitly marked steps_per_hour. P005 also has three unmarked rows. | Validate the rate interpretation before converting marked values; do not apply a blanket participant-level conversion. |
| Missing minute coverage | 6,840 missing expected minutes in each source. P010, P019, P022, P025 each lack 1,440; P020, P028, P041 each lack 360. | Preserve missingness and publish coverage; do not substitute zeros. |
| Duplicate minute keys | 5,767 excess rows per source. 3,585 step timestamp groups and 5,178 heart-rate groups contain conflicting values. | Separate identical duplicates from conflicts; define a defensible resolution policy. |
| Off-grid timestamps | 28 rows per source have nonzero seconds or microseconds. | Inspect before rounding; rounding can create more conflicts. |
| Mixed timestamp formats | 2,880 heart-rate rows have explicit timezone offsets, concentrated in P018 and P044. | Normalize explicit timestamps to study local UTC-05:00; preserve original strings and UTC instants. |
| Invalid measurements | Steps range from -1 to 1,000,000, with two negatives. Four heart-rate values fall outside the dictionary's 40-200 range; observed range is -10 to 999. | Flag and quarantine invalid values; select step anomaly rules after unit normalization. |
| Suspected sensor flatlines | P003 repeats 60 bpm for 180 consecutive minutes; P042 repeats 73 bpm for 180 minutes. | Flag as candidates, inspect activity/sleep context; constant values alone do not prove sensor failure. |
| Sleep stage duration contradictions | Four deep stages disagree with their timestamps: P007/Jan 21 (372 vs 194 min), P010/Jan 20 (307 vs 144), P024/Jan 11 (394 vs 200), P035/Jan 17 (302 vs 150). | Retain both values; consider deriving duration from timestamps after validating the timeline. |
| Sleep boundary convention | 1,397 sessions end their final non-awake stage exactly at sleep_end, then include an awake stage after that boundary. Three sessions violate that pattern. | Resolve semantics before defining time-in-bed and sleep efficiency; do not reject all sessions on a naive boundary rule. |

Steps and heart rate have identical timestamp-key sets after timezone normalization. This agreement does not establish correctness: both can share the same gaps and duplicate timestamps.

Participants and device metadata have no duplicate participant keys, missing participant coverage, or mismatched device assignments. Chronotypes are A: 28 and B: 22. Participants have no blank fields. Calibration dates precede the study start.

## Important interpretation limits

- Values above 30 steps/minute are candidates only; the dictionary explicitly allows activity bursts. The raw count of 95,902 such rows also includes mixed units.
- The 60-minute flatline threshold is an exploratory heuristic, not a validated acceptance rule.
- Counting stage ends after sleep_end yields a widespread diagnostic pattern. It is not evidence of 1,400 independent corrupt sessions.
- Using non-awake duration / (sleep_end - sleep_onset) yields 218 discrepancies above one percentage point against efficiency_pct. Using all stage durations as denominator yields 1,393. Neither formula is accepted until the awake-stage convention is understood.
- Duplicate-key counts are excess rows, while conflicting-value counts are timestamp groups. These measures are not interchangeable.
- The profiler assumes the supplied participant CSV and expected file paths exist. It is an exploratory tool, not production schema enforcement.

## Modeling implications

Preserve source records and lineage before correction. Unknown participants must not silently create valid business entities. Timestamp conflicts are data-quality events, not automatically satellite history. Define event time separately from vault load time, and never collapse conflicting observations without a recorded rule.

Kimball facts need declared grains and coverage measures. Compute weekly metrics from validated shared daily/minute rules, with explicit denominators and missing-data behavior. Delay final vault and fact design until units, conflicting keys, and sleep semantics are resolved.

## Reproduction

Run `python scripts/profile_data.py` from the project root with Python 3. The script uses only the standard library and writes `output/profiling/profile.json`, excluded from Git. It parses the complete minute dataset and may use substantial memory. Original files remain unchanged.
