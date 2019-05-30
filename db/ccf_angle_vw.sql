CREATE OR REPLACE VIEW ccf_angle_vw AS

WITH data AS (
	SELECT track_code,
		race_dt,
		race_num,
    day_eve_flag,
		is_route,
		surface,
		is_dirt,
		is_turf,
		is_allweather,
		is_maiden_race,
		is_graded_stakes_race,
		is_stakes_race,
		age_sex_restrictions,
		is_statebred,
		race_class,
		purse,
		field_size,
		track_condition,
		horse_name horse,
		post_position pp,
		program_number pn,
		jockey,
		trainer,
		claim_price,
		is_claiming_race,
		has_lasix,
		has_blinkers,
		odds,
		is_favorite,
		is_claimed,
		win_payoff,
		place_payoff,
		show_payoff,
		finish_pos,
		official_finish_pos,
		days_since_race,
		race_year,
		race_month,
		--
		-- TRAINERS
		--
		LEAD(trainer, 1) OVER (PARTITION BY horse_name ORDER BY race_dt DESC) trainer1,
		LEAD(trainer, 2) OVER (PARTITION BY horse_name ORDER BY race_dt DESC) trainer2,
		LEAD(trainer, 3) OVER (PARTITION BY horse_name ORDER BY race_dt DESC) trainer3,
		--
		-- JOCKEYS
		--
		LEAD(jockey, 1) OVER (PARTITION BY horse_name ORDER BY race_dt DESC) jockey1,
		LEAD(jockey, 2) OVER (PARTITION BY horse_name ORDER BY race_dt DESC) jockey2,
		LEAD(jockey, 3) OVER (PARTITION BY horse_name ORDER BY race_dt DESC) jockey3,
		--
		-- RACE DATES
		--
		LEAD(race_dt, 1) OVER (PARTITION BY horse_name ORDER BY race_dt DESC) racedate1,
		LEAD(race_dt, 2) OVER (PARTITION BY horse_name ORDER BY race_dt DESC) racedate2,
		LEAD(race_dt, 3) OVER (PARTITION BY horse_name ORDER BY race_dt DESC) racedate3,
		--
		-- DAYS BETWEEN RACE
		--
		to_date(race_dt, 'YYYYMMDD') - 
		to_date(LEAD(race_dt, 1) OVER (PARTITION BY horse_name ORDER BY race_dt DESC), 'YYYYMMDD') days_since_race1,
		to_date(LEAD(race_dt, 1) OVER (PARTITION BY horse_name ORDER BY race_dt DESC), 'YYYYMMDD') - 
		to_date(LEAD(race_dt, 2) OVER (PARTITION BY horse_name ORDER BY race_dt DESC), 'YYYYMMDD') days_since_race2,
		to_date(LEAD(race_dt, 2) OVER (PARTITION BY horse_name ORDER BY race_dt DESC), 'YYYYMMDD') - 
		to_date(LEAD(race_dt, 3) OVER (PARTITION BY horse_name ORDER BY race_dt DESC), 'YYYYMMDD') days_since_race3,
		--
		-- ROUTE/SPRINT
		--
		LEAD(is_route, 1) OVER (PARTITION BY horse_name ORDER BY race_dt DESC) is_route1,
		LEAD(is_route, 2) OVER (PARTITION BY horse_name ORDER BY race_dt DESC) is_route2,
		LEAD(is_route, 3) OVER (PARTITION BY horse_name ORDER BY race_dt DESC) is_route3,
		--
		-- TURF/DIRT
		--
		LEAD(is_turf, 1) OVER (PARTITION BY horse_name ORDER BY race_dt DESC) is_turf1,
		LEAD(is_turf, 2) OVER (PARTITION BY horse_name ORDER BY race_dt DESC) is_turf2,
		LEAD(is_turf, 3) OVER (PARTITION BY horse_name ORDER BY race_dt DESC) is_turf3,
		--
		-- FINISHES
		--
		LEAD(official_finish_pos, 1) OVER (PARTITION BY horse_name ORDER BY race_dt DESC) finish1,
		LEAD(official_finish_pos, 2) OVER (PARTITION BY horse_name ORDER BY race_dt DESC) finish2,
		LEAD(official_finish_pos, 3) OVER (PARTITION BY horse_name ORDER BY race_dt DESC) finish3,
		--
		-- CLAIMED
		--
		LEAD(is_claimed, 1) OVER (PARTITION BY horse_name ORDER BY race_dt DESC) is_claimed1,
		LEAD(is_claimed, 2) OVER (PARTITION BY horse_name ORDER BY race_dt DESC) is_claimed2,
		LEAD(is_claimed, 3) OVER (PARTITION BY horse_name ORDER BY race_dt DESC) is_claimed3,
		--
		-- BLINKERS
		--
		LEAD(has_blinkers, 1) OVER (PARTITION BY horse_name ORDER BY race_dt DESC) has_blinkers1,
		LEAD(has_blinkers, 2) OVER (PARTITION BY horse_name ORDER BY race_dt DESC) has_blinkers2,
		LEAD(has_blinkers, 3) OVER (PARTITION BY horse_name ORDER BY race_dt DESC) has_blinkers3,
		--
		-- LASIX
		--
		LEAD(has_lasix, 1) OVER (PARTITION BY horse_name ORDER BY race_dt DESC) has_lasix1,
		LEAD(has_lasix, 2) OVER (PARTITION BY horse_name ORDER BY race_dt DESC) has_lasix2,
		LEAD(has_lasix, 3) OVER (PARTITION BY horse_name ORDER BY race_dt DESC) has_lasix3,
		--
		-- CLAIMING RACE
		--
		LEAD(is_claiming_race, 1) OVER (PARTITION BY horse_name ORDER BY race_dt DESC) is_claiming_race1,
		LEAD(is_claiming_race, 2) OVER (PARTITION BY horse_name ORDER BY race_dt DESC) is_claiming_race2,
		LEAD(is_claiming_race, 3) OVER (PARTITION BY horse_name ORDER BY race_dt DESC) is_claiming_race3,
		--
		-- MAIDEN RACE
		--
		LEAD(is_maiden_race, 1) OVER (PARTITION BY horse_name ORDER BY race_dt DESC) is_maiden_race1,
		LEAD(is_maiden_race, 2) OVER (PARTITION BY horse_name ORDER BY race_dt DESC) is_maiden_race2,
		LEAD(is_maiden_race, 3) OVER (PARTITION BY horse_name ORDER BY race_dt DESC) is_maiden_race3
  FROM ccf_vw
)
--
-- TRAINERS: 1ST OFF CLAIM
--
SELECT track_code,
  race_dt,
  race_num,
  day_eve_flag,
  horse,
  '1CL' angle
FROM data
WHERE is_claimed1 = 1
UNION ALL

--
-- TRAINERS: 2ND OFF CLAIM
--
SELECT track_code,
  race_dt,
  race_num,
  day_eve_flag,
  horse,
  '2CL' angle
FROM data
WHERE is_claimed2 = 1
  AND is_claimed1 = 0
  AND trainer = trainer1
UNION ALL

--
-- TRAINERS: 1ST OFF BARN CHANGE (NOT CLAIMED)
--
SELECT track_code,
  race_dt,
  race_num,
  day_eve_flag,
  horse,
  '1BC' angle
FROM data
WHERE is_claimed1 = 0
  AND trainer1 IS NOT NULL
	AND trainer <> trainer1
UNION ALL

--
-- TRAINERS: 2ND OFF BARN CHANGE (NOT CLAIMED)
--
SELECT track_code,
  race_dt,
  race_num,
  day_eve_flag,
  horse,
  '2BC' angle
FROM data
WHERE is_claimed1 = 0
  AND is_claimed2 = 0
	AND trainer = trainer1
	AND trainer2 IS NOT NULL
	AND trainer <> trainer2
UNION ALL

--
-- TRAINERS: 1ST TIME BLINKERS ON
--
SELECT track_code,
  race_dt,
  race_num,
  day_eve_flag,
  horse,
  '1BON' angle
FROM data
WHERE has_blinkers = 1
  AND has_blinkers1 = 0
UNION ALL

--
-- TRAINERS: 2ND TIME BLINKERS ON
--
SELECT track_code,
  race_dt,
  race_num,
  day_eve_flag,
  horse,
  '2BON' angle
FROM data
WHERE has_blinkers2 = 0
  AND has_blinkers1 = 1
	AND has_blinkers = 1
	AND trainer = trainer1
UNION ALL

--
-- TRAINERS: 1ST TIME BLINKERS OFF
--
SELECT track_code,
  race_dt,
  race_num,
  day_eve_flag,
  horse,
  '1BOFF' angle
FROM data
WHERE has_blinkers = 0
  AND has_blinkers1 = 1
UNION ALL

--
-- TRAINERS: 2ND TIME BLINKERS OFF
--
SELECT track_code,
  race_dt,
  race_num,
  day_eve_flag,
  horse,
  '2BOFF' angle
FROM data
WHERE has_blinkers2 = 1
  AND has_blinkers1 = 0
	AND has_blinkers = 0
	AND trainer = trainer1
UNION ALL

--
-- TRAINERS: LESS THAN 15 DAYS
--
SELECT track_code,
  race_dt,
  race_num,
  day_eve_flag,
  horse,
  '15DAYS' angle
FROM data
WHERE days_since_race1 <= 15
UNION ALL

--
-- TRAINERS: 1ST OFF LAYOFF (> 31-60 DAYS)
--
SELECT track_code,
  race_dt,
  race_num,
  day_eve_flag,
  horse,
  '1LO30' angle
FROM data
WHERE days_since_race1 BETWEEN 31 AND 60
UNION ALL

--
-- TRAINERS: 2ND OFF LAYOFF (> 30-60 DAYS)
--
SELECT track_code,
  race_dt,
  race_num,
  day_eve_flag,
  horse,
  '2LO30' angle
FROM data
WHERE days_since_race1 <= 45
  AND days_since_race2 BETWEEN 31 AND 60
	AND trainer = trainer1
UNION ALL

--
-- TRAINERS: 1ST OFF LAYOFF (> 60-180 DAYS)
--
SELECT track_code,
  race_dt,
  race_num,
  day_eve_flag,
  horse,
  '1LO60' angle
FROM data
WHERE days_since_race1 BETWEEN 61 AND 180
UNION ALL

--
-- TRAINERS: 2ND OFF LAYOFF (> 60-180 DAYS)
--
SELECT track_code,
  race_dt,
  race_num,
  day_eve_flag,
  horse,
  '2LO60' angle
FROM data
WHERE days_since_race1 <= 45
	AND days_since_race2 BETWEEN 61 AND 180
	AND trainer = trainer1
UNION ALL

--
-- TRAINERS: 1ST OFF LAYOFF (> 180 DAYS)
--
SELECT track_code,
  race_dt,
  race_num,
  day_eve_flag,
  horse,
  '1LO180' angle
FROM data
WHERE days_since_race1 > 180
UNION ALL

--
-- TRAINERS: 2ND OFF LAYOFF (> 180 DAYS)
--
SELECT track_code,
  race_dt,
  race_num,
  day_eve_flag,
  horse,
  '2LO180' angle
FROM data
WHERE days_since_race1 <= 45
	AND days_since_race2 > 180
	AND trainer = trainer1
UNION ALL

--
-- TRAINERS: 1ST LASIX ON
--
SELECT track_code,
  race_dt,
  race_num,
  day_eve_flag,
  horse,
  '1LON' angle
FROM data
WHERE has_lasix = 1
	AND has_lasix1 = 0
UNION ALL

--
-- TRAINERS: 2ND LASIX ON
--
SELECT track_code,
  race_dt,
  race_num,
  day_eve_flag,
  horse,
  '2LON' angle
FROM data
WHERE has_lasix2 = 0
	AND has_lasix1 = 1
	AND has_lasix = 1
	AND trainer = trainer1
UNION ALL

--
-- TRAINERS: SPRINT TO ROUTE
--
SELECT track_code,
  race_dt,
  race_num,
  day_eve_flag,
  horse,
  'S2R' angle
FROM data
WHERE is_route = 1
	AND is_route1 = 0
UNION ALL

--
-- TRAINERS: ROUTE TO SPRINT
--
SELECT track_code,
  race_dt,
  race_num,
  day_eve_flag,
  horse,
  'R2S' angle
FROM data
WHERE is_route = 0
	AND is_route1 = 1
UNION ALL

--
-- TRAINERS: DIRT TO TURF
--
SELECT track_code,
  race_dt,
  race_num,
  day_eve_flag,
  horse,
  'D2T' angle
FROM data
WHERE is_turf = 1
	AND is_turf1 = 0
UNION ALL

--
-- TRAINERS: TURF TO DIRT
--
SELECT track_code,
  race_dt,
  race_num,
  day_eve_flag,
  horse,
  'T2D' angle
FROM data
WHERE is_turf = 0
  AND is_turf1 = 1
UNION ALL

--
-- JOCKEYS: OUT OF MONEY LAST (STAYS ON)
--
SELECT jockey,
	'jockey',
	'OTM-STAY',
	COUNT(*),
	ROUND(SUM(CASE WHEN official_pos = 1 THEN 1 ELSE 0 END)/CAST(COUNT(*) AS FLOAT) * 100),
	ROUND(SUM(CASE WHEN official_pos IN (1, 2, 3) THEN 1 ELSE 0 END)/CAST(COUNT(*) AS FLOAT) * 100),
	ROUND(MEDIAN(odds))
FROM (
	SELECT *,
		ROW_NUMBER() OVER (PARTITION BY jockey ORDER BY racedate DESC, racenum DESC) rn
	FROM (
		SELECT jockey, official_pos, odds, horse, racedate, racenum
		FROM data
		WHERE jockey = jockey1
			AND finish1 > 3
	) z
) a
WHERE rn <= 25
GROUP BY jockey
HAVING COUNT(*) >= 5
UNION ALL

--
-- JOCKEYS: SWITCH TO (ALL)
--
SELECT jockey,
	'jockey',
	'SWITCH',
	COUNT(*),
	ROUND(SUM(CASE WHEN official_pos = 1 THEN 1 ELSE 0 END)/CAST(COUNT(*) AS FLOAT) * 100),
	ROUND(SUM(CASE WHEN official_pos IN (1, 2, 3) THEN 1 ELSE 0 END)/CAST(COUNT(*) AS FLOAT) * 100),
	ROUND(MEDIAN(odds))
FROM (
	SELECT *,
		ROW_NUMBER() OVER (PARTITION BY jockey ORDER BY racedate DESC, racenum DESC) rn
	FROM (
		SELECT jockey, official_pos, odds, horse, racedate, racenum
		FROM data
		WHERE jockey1 IS NOT NULL 
			AND jockey NOT IN (jockey1, COALESCE(jockey2, ''), COALESCE(jockey3, ''))
	) z
) a
WHERE rn <= 25
GROUP BY jockey
HAVING COUNT(*) >= 5
UNION ALL

--
-- JOCKEYS: SWITCH TO (ON DIRT)
--
SELECT jockey,
	'jockey',
	'SWITCH-DRT',
	COUNT(*),
	ROUND(SUM(CASE WHEN official_pos = 1 THEN 1 ELSE 0 END)/CAST(COUNT(*) AS FLOAT) * 100),
	ROUND(SUM(CASE WHEN official_pos IN (1, 2, 3) THEN 1 ELSE 0 END)/CAST(COUNT(*) AS FLOAT) * 100),
	ROUND(MEDIAN(odds))
FROM (
	SELECT *,
		ROW_NUMBER() OVER (PARTITION BY jockey ORDER BY racedate DESC, racenum DESC) rn
	FROM (
		SELECT jockey, official_pos, odds, horse, racedate, racenum
		FROM data
		WHERE jockey1 IS NOT NULL 
			AND jockey NOT IN (jockey1, COALESCE(jockey2, ''), COALESCE(jockey3, ''))
			AND is_turf = 0
	) z
) a
WHERE rn <= 25
GROUP BY jockey
HAVING COUNT(*) >= 5
UNION ALL

--
-- JOCKEYS: SWITCH TO (ON TURF)
--
SELECT jockey,
	'jockey',
	'SWITCH-TRF',
	COUNT(*),
	ROUND(SUM(CASE WHEN official_pos = 1 THEN 1 ELSE 0 END)/CAST(COUNT(*) AS FLOAT) * 100),
	ROUND(SUM(CASE WHEN official_pos IN (1, 2, 3) THEN 1 ELSE 0 END)/CAST(COUNT(*) AS FLOAT) * 100),
	ROUND(MEDIAN(odds))
FROM (
	SELECT *,
		ROW_NUMBER() OVER (PARTITION BY jockey ORDER BY racedate DESC, racenum DESC) rn
	FROM (
		SELECT jockey, official_pos, odds, horse, racedate, racenum
		FROM data
		WHERE jockey1 IS NOT NULL 
			AND jockey NOT IN (jockey1, COALESCE(jockey2, ''), COALESCE(jockey3, ''))
			AND is_turf = 1
	) z
) a
WHERE rn <= 25
GROUP BY jockey
HAVING COUNT(*) >= 5
UNION ALL

--
-- JOCKEYS: SWITCH TO (OTM LAST)
--
SELECT jockey,
	'jockey',
	'SWITCH-OTM',
	COUNT(*),
	ROUND(SUM(CASE WHEN official_pos = 1 THEN 1 ELSE 0 END)/CAST(COUNT(*) AS FLOAT) * 100),
	ROUND(SUM(CASE WHEN official_pos IN (1, 2, 3) THEN 1 ELSE 0 END)/CAST(COUNT(*) AS FLOAT) * 100),
	ROUND(MEDIAN(odds))
FROM (
	SELECT *,
		ROW_NUMBER() OVER (PARTITION BY jockey ORDER BY racedate DESC, racenum DESC) rn
	FROM (
		SELECT jockey, official_pos, odds, horse, racedate, racenum
		FROM data
		WHERE jockey1 IS NOT NULL 
			AND jockey NOT IN (jockey1, COALESCE(jockey2, ''), COALESCE(jockey3, ''))
			AND finish1 > 3
	) z
) a
WHERE rn <= 25
GROUP BY jockey
HAVING COUNT(*) >= 5
UNION ALL

--
-- JOCKEYS: SWITCH TO (SPRINTS)
--
SELECT jockey,
	'jockey',
	'SWITCH-SPR',
	COUNT(*),
	ROUND(SUM(CASE WHEN official_pos = 1 THEN 1 ELSE 0 END)/CAST(COUNT(*) AS FLOAT) * 100),
	ROUND(SUM(CASE WHEN official_pos IN (1, 2, 3) THEN 1 ELSE 0 END)/CAST(COUNT(*) AS FLOAT) * 100),
	ROUND(MEDIAN(odds))
FROM (
	SELECT *,
		ROW_NUMBER() OVER (PARTITION BY jockey ORDER BY racedate DESC, racenum DESC) rn
	FROM (
		SELECT jockey, official_pos, odds, horse, racedate, racenum
		FROM data
		WHERE jockey1 IS NOT NULL 
			AND jockey NOT IN (jockey1, COALESCE(jockey2, ''), COALESCE(jockey3, ''))
			AND is_route = 0
	) z
) a
WHERE rn <= 25
GROUP BY jockey
HAVING COUNT(*) >= 5
UNION ALL

--
-- JOCKEYS: SWITCH TO (ROUTES)
--
SELECT jockey,
	'jockey',
	'SWITCH-RTE',
	COUNT(*),
	ROUND(SUM(CASE WHEN official_pos = 1 THEN 1 ELSE 0 END)/CAST(COUNT(*) AS FLOAT) * 100),
	ROUND(SUM(CASE WHEN official_pos IN (1, 2, 3) THEN 1 ELSE 0 END)/CAST(COUNT(*) AS FLOAT) * 100),
	ROUND(MEDIAN(odds))
FROM (
	SELECT *,
		ROW_NUMBER() OVER (PARTITION BY jockey ORDER BY racedate DESC, racenum DESC) rn
	FROM (
		SELECT jockey, official_pos, odds, horse, racedate, racenum
		FROM data
		WHERE jockey1 IS NOT NULL 
			AND jockey NOT IN (jockey1, COALESCE(jockey2, ''), COALESCE(jockey3, ''))
			AND is_route = 1
	) z
) a
WHERE rn <= 25
GROUP BY jockey
HAVING COUNT(*) >= 5
UNION ALL

--
-- TRAINER/JOCKEY: LAST 5 STARTS
--
SELECT trainer || '/' || jockey,
	'trainer/jockey',
	'L5',
	COUNT(*),
	ROUND(SUM(CASE WHEN official_pos = 1 THEN 1 ELSE 0 END)/CAST(COUNT(*) AS FLOAT) * 100),
	ROUND(SUM(CASE WHEN official_pos IN (1, 2, 3) THEN 1 ELSE 0 END)/CAST(COUNT(*) AS FLOAT) * 100),
	ROUND(MEDIAN(odds))
FROM data
WHERE rn_trainer_jockey <= 5
GROUP BY trainer, jockey
HAVING COUNT(*) > 2
UNION ALL

--
-- TRAINER/JOCKEY: LAST 10 STARTS
--
SELECT trainer || '/' || jockey,
	'trainer/jockey',
	'L10',
	COUNT(*),
	ROUND(SUM(CASE WHEN official_pos = 1 THEN 1 ELSE 0 END)/CAST(COUNT(*) AS FLOAT) * 100),
	ROUND(SUM(CASE WHEN official_pos IN (1, 2, 3) THEN 1 ELSE 0 END)/CAST(COUNT(*) AS FLOAT) * 100),
	ROUND(MEDIAN(odds))
FROM data
WHERE rn_trainer_jockey <= 10
GROUP BY trainer, jockey
HAVING COUNT(*) > 5
UNION ALL

--
-- TRAINER/JOCKEY: LAST 15 STARTS
--
SELECT trainer || '/' || jockey,
	'trainer/jockey',
	'L15',
	COUNT(*),
	ROUND(SUM(CASE WHEN official_pos = 1 THEN 1 ELSE 0 END)/CAST(COUNT(*) AS FLOAT) * 100),
	ROUND(SUM(CASE WHEN official_pos IN (1, 2, 3) THEN 1 ELSE 0 END)/CAST(COUNT(*) AS FLOAT) * 100),
	ROUND(MEDIAN(odds))
FROM data
WHERE rn_trainer_jockey <= 15
GROUP BY trainer, jockey
HAVING COUNT(*) > 7
UNION ALL

--
-- TRAINER/JOCKEY: OUT OF MONEY LAST (STAYS ON)
--
SELECT trainer || '/' || jockey,
	'trainer/jockey',
	'OTM-STAY',
	COUNT(*),
	ROUND(SUM(CASE WHEN official_pos = 1 THEN 1 ELSE 0 END)/CAST(COUNT(*) AS FLOAT) * 100),
	ROUND(SUM(CASE WHEN official_pos IN (1, 2, 3) THEN 1 ELSE 0 END)/CAST(COUNT(*) AS FLOAT) * 100),
	ROUND(MEDIAN(odds))
FROM (
	SELECT *,
		ROW_NUMBER() OVER (PARTITION BY trainer, jockey ORDER BY racedate DESC, racenum DESC) rn
	FROM (
		SELECT trainer, jockey, official_pos, odds, horse, racedate, racenum
		FROM data
		WHERE jockey = jockey1
			AND finish1 > 3
	) z
) a
WHERE rn <= 25
GROUP BY trainer, jockey
HAVING COUNT(*) >= 5
UNION ALL

--
-- TRAINER/JOCKEY: SWITCH TO (ALL)
--
SELECT trainer || '/' || jockey,
	'trainer/jockey',
	'SWITCH',
	COUNT(*),
	ROUND(SUM(CASE WHEN official_pos = 1 THEN 1 ELSE 0 END)/CAST(COUNT(*) AS FLOAT) * 100),
	ROUND(SUM(CASE WHEN official_pos IN (1, 2, 3) THEN 1 ELSE 0 END)/CAST(COUNT(*) AS FLOAT) * 100),
	ROUND(MEDIAN(odds))
FROM (
	SELECT *,
		ROW_NUMBER() OVER (PARTITION BY trainer, jockey ORDER BY racedate DESC, racenum DESC) rn
	FROM (
		SELECT trainer, jockey, official_pos, odds, horse, racedate, racenum
		FROM data
		WHERE jockey1 IS NOT NULL 
			AND jockey NOT IN (jockey1, COALESCE(jockey2, ''), COALESCE(jockey3, ''))
	) z
) a
WHERE rn <= 25
GROUP BY trainer, jockey
HAVING COUNT(*) >= 5
UNION ALL

--
-- TRAINER/JOCKEY: SWITCH TO (ON DIRT)
--
SELECT trainer || '/' || jockey,
	'trainer/jockey',
	'SWITCH-DRT',
	COUNT(*),
	ROUND(SUM(CASE WHEN official_pos = 1 THEN 1 ELSE 0 END)/CAST(COUNT(*) AS FLOAT) * 100),
	ROUND(SUM(CASE WHEN official_pos IN (1, 2, 3) THEN 1 ELSE 0 END)/CAST(COUNT(*) AS FLOAT) * 100),
	ROUND(MEDIAN(odds))
FROM (
	SELECT *,
		ROW_NUMBER() OVER (PARTITION BY trainer, jockey ORDER BY racedate DESC, racenum DESC) rn
	FROM (
		SELECT trainer, jockey, official_pos, odds, horse, racedate, racenum
		FROM data
		WHERE jockey1 IS NOT NULL 
			AND jockey NOT IN (jockey1, COALESCE(jockey2, ''), COALESCE(jockey3, ''))
			AND is_turf = 0
	) z
) a
WHERE rn <= 25
GROUP BY trainer, jockey
HAVING COUNT(*) >= 5
UNION ALL

--
-- TRAINER/JOCKEY: SWITCH TO (ON TURF)
--
SELECT trainer || '/' || jockey,
	'trainer/jockey',
	'SWITCH-TRF',
	COUNT(*),
	ROUND(SUM(CASE WHEN official_pos = 1 THEN 1 ELSE 0 END)/CAST(COUNT(*) AS FLOAT) * 100),
	ROUND(SUM(CASE WHEN official_pos IN (1, 2, 3) THEN 1 ELSE 0 END)/CAST(COUNT(*) AS FLOAT) * 100),
	ROUND(MEDIAN(odds))
FROM (
	SELECT *,
		ROW_NUMBER() OVER (PARTITION BY trainer, jockey ORDER BY racedate DESC, racenum DESC) rn
	FROM (
		SELECT trainer, jockey, official_pos, odds, horse, racedate, racenum
		FROM data
		WHERE jockey1 IS NOT NULL 
			AND jockey NOT IN (jockey1, COALESCE(jockey2, ''), COALESCE(jockey3, ''))
			AND is_turf = 1
	) z
) a
WHERE rn <= 25
GROUP BY trainer, jockey
HAVING COUNT(*) >= 5
UNION ALL

--
-- TRAINER/JOCKEY: SWITCH TO (OTM LAST)
--
SELECT trainer || '/' || jockey,
	'trainer/jockey',
	'SWITCH-OTM',
	COUNT(*),
	ROUND(SUM(CASE WHEN official_pos = 1 THEN 1 ELSE 0 END)/CAST(COUNT(*) AS FLOAT) * 100),
	ROUND(SUM(CASE WHEN official_pos IN (1, 2, 3) THEN 1 ELSE 0 END)/CAST(COUNT(*) AS FLOAT) * 100),
	ROUND(MEDIAN(odds))
FROM (
	SELECT *,
		ROW_NUMBER() OVER (PARTITION BY trainer, jockey ORDER BY racedate DESC, racenum DESC) rn
	FROM (
		SELECT trainer, jockey, official_pos, odds, horse, racedate, racenum
		FROM data
		WHERE jockey1 IS NOT NULL 
			AND jockey NOT IN (jockey1, COALESCE(jockey2, ''), COALESCE(jockey3, ''))
			AND finish1 > 3
	) z
) a
WHERE rn <= 25
GROUP BY trainer, jockey
HAVING COUNT(*) >= 5
UNION ALL

--
-- TRAINER/JOCKEY: SWITCH TO (SPRINTS)
--
SELECT trainer || '/' || jockey,
	'trainer/jockey',
	'SWITCH-SPR',
	COUNT(*),
	ROUND(SUM(CASE WHEN official_pos = 1 THEN 1 ELSE 0 END)/CAST(COUNT(*) AS FLOAT) * 100),
	ROUND(SUM(CASE WHEN official_pos IN (1, 2, 3) THEN 1 ELSE 0 END)/CAST(COUNT(*) AS FLOAT) * 100),
	ROUND(MEDIAN(odds))
FROM (
	SELECT *,
		ROW_NUMBER() OVER (PARTITION BY trainer, jockey ORDER BY racedate DESC, racenum DESC) rn
	FROM (
		SELECT trainer, jockey, official_pos, odds, horse, racedate, racenum
		FROM data
		WHERE jockey1 IS NOT NULL 
			AND jockey NOT IN (jockey1, COALESCE(jockey2, ''), COALESCE(jockey3, ''))
			AND is_route = 0
	) z
) a
WHERE rn <= 25
GROUP BY trainer, jockey
HAVING COUNT(*) >= 5
UNION ALL

--
-- TRAINER/JOCKEY: SWITCH TO (ROUTES)
--
SELECT trainer || '/' || jockey,
	'trainer/jockey',
	'SWITCH-RTE',
	COUNT(*),
	ROUND(SUM(CASE WHEN official_pos = 1 THEN 1 ELSE 0 END)/CAST(COUNT(*) AS FLOAT) * 100),
	ROUND(SUM(CASE WHEN official_pos IN (1, 2, 3) THEN 1 ELSE 0 END)/CAST(COUNT(*) AS FLOAT) * 100),
	ROUND(MEDIAN(odds))
FROM (
	SELECT *,
		ROW_NUMBER() OVER (PARTITION BY trainer, jockey ORDER BY racedate DESC, racenum DESC) rn
	FROM (
		SELECT trainer, jockey, official_pos, odds, horse, racedate, racenum
		FROM data
		WHERE jockey1 IS NOT NULL 
			AND jockey NOT IN (jockey1, COALESCE(jockey2, ''), COALESCE(jockey3, ''))
			AND is_route = 1
	) z
) a
WHERE rn <= 25
GROUP BY trainer, jockey
HAVING COUNT(*) >= 5
