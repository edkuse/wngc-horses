CREATE TABLE angles (
  track_code varchar(3) not null,
  race_dt varchar(8) not null,
  race_num integer not null,
  day_eve_flag varchar(1) not null,
  horse_name varchar(50) not null,
  angle varchar(25) not null,
  primary key (track_code, race_dt, race_num, day_eve_flag, horse_name, angle)
);

CREATE INDEX angles_idx1 ON angles (angle);


CREATE OR REPLACE VIEW ccf_angle_vw AS

SELECT a.track_code,
  a.race_dt,
  a.race_num,
  a.horse_name,
  b.angle,
  a.distance,
  a.furlongs,
  a.is_about_distance,
  a.is_route,
  a.surface,
  a.is_dirt,
  a.is_turf,
  a.is_allweather,
  a.track_condition,
  a.jockey,
  a.trainer,
  a.is_claiming_race,
  a.odds,
  a.is_favorite,
  a.win_payoff,
  a.place_payoff,
  a.show_payoff,
  a.win_profit,
  a.itm_profit,
  a.official_finish_pos,
  a.days_since_race,
  a.race_year,
  a.race_month,
  a.rn_trainer,
  a.rn_jockey,
  a.rn_trainer_jockey
FROM ccf_vw a 
INNER JOIN angles b 
  ON b.track_code = a.track_code AND 
     b.race_dt = a.race_dt AND
     b.race_num = a.race_num AND
     b.horse_name = a.horse_name;
