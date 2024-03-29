-- Copyright 2023 Google LLC

-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at

--     https://www.apache.org/licenses/LICENSE-2.0

-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

SELECT
name as region_name, total_trips
FROM `packt-data-eng-on-gcp.dwh_bikesharing.fact_region_gender_daily`fact
JOIN `packt-data-eng-on-gcp.dwh_bikesharing.dim_regions` dim
ON fact.region_id = dim.region_id
WHERE DATE(trip_date) = DATE('2018-01-02')
AND member_gender = 'Female'
ORDER BY total_trips desc
LIMIT 3;
