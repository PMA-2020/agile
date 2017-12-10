"""Source data.

- organisationUnits/all_level_3.csv
  - URL: https://hiskenya.org/api/organisationUnits?filter=level:eq:3&paging=False&fields=:all&format=csv
- indicators/
  - URL: https://hiskenya.org/api/indicatorGroups.json?paging=False&filter=name
  :ilike:{{name_starts_with}}&fields=name,id,indicators[id,name,numerator,
  denominator]
    - indicators_by_group_01.json, name_starts_with = FP
    - indicators_by_group_01.json, name_starts_with = Family%20Planning
"""
