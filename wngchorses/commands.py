from flask import current_app
from flask.cli import with_appcontext
from sqlalchemy import text
import click
import csv
import os
import subprocess
import zipfile
from wngchorses.extensions import db
from wngchorses.ccf.models import Race, Starter, Track


@click.command()
@with_appcontext
def create_db():
    """
    Creates db tables - import your models within commands.py to create the models.
    """
    db.drop_all()
    db.create_all()

    db.engine.execute(
        Track.__table__.insert(), [
            {'track_code': 'DMR', 'ccf_code': 'dmr', 'track_nm': 'Del Mar'},
            {'track_code': 'GG', 'ccf_code': 'ggx', 'track_nm': 'Golden Gate'},
            {'track_code': 'LRC', 'ccf_code': 'lrc', 'track_nm': 'Los Alamitos'},
            {'track_code': 'OTP', 'ccf_code': 'otp', 'track_nm': 'Pleasanton'},
            {'track_code': 'SAC', 'ccf_code': 'sac', 'track_nm': 'Sacramento'},
            {'track_code': 'SA', 'ccf_code': 'sax', 'track_nm': 'Santa Anita'}
        ]
    )

    with current_app.open_resource('../db/ccf_vw.sql') as f:
        db.engine.execute(text(f.read().decode('utf8')))

    print ('Database structure created successfully')


@click.command()
@click.option('-f', '--file', prompt=True, default=None)
@with_appcontext
def load_ccf(file):
    """
    Load Brisnet chart file (ccf) to database
    """
    f = os.path.basename(file)
    print(f)

    track = f[:3]
    race_date = f'{f[7:11]}{f[3:5]}{f[5:7]}'

    # GET THE PP/TRACK CODE
    q = Track.find_by_ccf(track)
    track_code = q.track_code

    # UNZIP THE CCF FILE
    files = []

    if not os.path.isfile(file):
        print("File {0} does not exist!\n\n".format(file))
        return

    unzip = zipfile.ZipFile(file)

    for name in unzip.namelist():
        fn = 'files/tmp/' + name
        files.append(fn)

        with open(fn, 'wb') as fd:
            fd.write(unzip.read(name))

    # DELETE ANY EXISTING DATA FOR DATE AND TRACK
    Race.find_by_track_date(track_code, race_date).delete()
    Starter.find_by_track_date(track_code, race_date).delete()

    # LOAD RACE LEVEL DATA
    with open(files[0], 'r') as rf:
        reader = csv.reader(rf)

        for r, row in enumerate(reader):
            race = Race()
            race.track_code = row[0]
            race.race_dt = row[1]
            race.race_num = row[2]
            race.day_eve_flag = row[3]
            race.distance = row[4]
            race.distance_unit = row[5]
            race.about_dist_flag = row[6] or None
            race.surface_1 = row[7]
            race.surface_2 = row[8]
            race.all_weather_flag = row[10] or None
            race.chute_start_flag = row[11] or None
            race.bris_racetype = row[12]
            race.eqb_racetype = row[13]
            race.race_grade = row[14]
            race.age_sex_restrictions = row[15] or None
            race.race_restrictions_code = row[16] or None
            race.statebred_flag = row[17] or None
            race.race_class = row[18] or None
            race.breed_ind = row[19] or None
            race.country_code = row[20] or None
            race.purse = row[21]
            race.total_value = row[22]
            race.max_claim_price = row[27]
            race.conditions_1 = row[29] or None
            race.conditions_2 = row[30] or None
            race.conditions_3 = row[31] or None
            race.conditions_4 = row[32] or None
            race.conditions_5 = row[33] or None
            race.field_size = row[36]
            race.track_condition = row[37] or None
            race.fraction_1 = row[38] or None
            race.fraction_2 = row[39] or None
            race.fraction_3 = row[40] or None
            race.fraction_4 = row[41] or None
            race.fraction_5 = row[42] or None
            race.final_time = row[43]
            race.fraction_1d = row[44] or None
            race.fraction_2d = row[45] or None
            race.fraction_3d = row[46] or None
            race.fraction_4d = row[47] or None
            race.fraction_5d = row[48] or None
            race.off_time = row[49] or None
            race.call_start_dist = row[50]
            race.call_1_dist = row[51]
            race.call_2_dist = row[52]
            race.call_3_dist = row[53]
            race.race_name = row[54] or None
            race.start_descr = row[55] or None
            race.temp_rail_dist = row[56]
            race.off_turf_ind = row[57] or None
            race.off_turf_dist_chg = row[58] or None
            race.weather = row[62] or None
            race.race_temp = row[63] or None
            race.wps_pool = float(row[64])
            race.runup_dist = row[65] or None

            db.session.add(race)

        db.session.commit()

    # LOAD STARTER LEVEL DATA
    with open(files[1], 'r') as sf:
        reader = csv.reader(sf)

        # HAVE FOUND BUG WHERE DUPLICATE STARTERS IN FILE
        pk = []

        for s, row in enumerate(reader):
            key = row[:5]

            if key in pk:
                continue
            pk.append(key)

            starter = Starter()
            starter.track_code = row[0]
            starter.race_dt = row[1]
            starter.race_num = row[2]
            starter.day_eve_flag = row[3]
            starter.horse_name = row[4].strip()
            starter.foreign_bred_code = row[5] or None
            starter.state_bred_code = row[6] or None
            starter.post_position = row[7]
            starter.program_number = row[8] or None
            starter.birth_year = row[9]
            starter.breed = row[10] or None
            starter.coupled_flag = row[11] or None
            starter.jockey = row[12].strip() or None
            starter.jockey_last = row[13].strip() or None
            starter.jockey_first = row[14].strip() or None
            starter.jockey_middle = row[15].strip() or None
            starter.trainer = row[17].strip() or None
            starter.trainer_last = row[18].strip() or None
            starter.trainer_first = row[19].strip() or None
            starter.trainer_middle = row[20].strip() or None
            starter.trip_comment = row[21] or None
            starter.owners = row[23].strip() or None
            starter.owner_first = row[24].strip() or None
            starter.owner_middle = row[25].strip() or None
            starter.claim_price = row[26]
            starter.medications = row[27] or None
            starter.equipment = row[28] or None
            starter.earnings = row[29]
            starter.odds = row[30]
            starter.non_betting_flag = row[31] or None
            starter.favorite_flag = row[32]
            starter.dq_flag = row[35] or None
            starter.dq_placing = row[36]
            starter.weight = row[37]
            starter.corrected_weight = row[38] or None
            starter.overweight_amount = row[39] or None
            starter.claimed_ind = row[40] or None
            starter.claimed_by_trainer = row[41].strip() or None
            starter.claimed_by_trainer_last = row[42].strip() or None
            starter.claimed_by_trainer_first = row[43].strip() or None
            starter.claimed_by_trainer_middle = row[44].strip() or None
            starter.claimed_by_owner = row[46].strip() or None
            starter.claimed_by_owner_last = row[47].strip() or None
            starter.claimed_by_owner_first = row[48].strip() or None
            starter.claimed_by_owner_middle = row[49].strip() or None
            starter.win_payoff = row[50] or None
            starter.place_payoff = row[51] or None
            starter.show_payoff = row[52] or None
            starter.call_start_pos = row[54] or None
            starter.call_1_pos = row[55] or None
            starter.call_2_pos = row[56] or None
            starter.call_3_pos = row[57] or None
            starter.stretch_pos = row[58] or None
            starter.finish_pos = row[59] or None
            starter.official_finish_pos = row[60] or None
            starter.call_start_la = row[61] or None
            starter.call_1_la = row[62] or None
            starter.call_2_la = row[63] or None
            starter.call_3_la = row[64] or None
            starter.stretch_la = row[65] or None
            starter.finish_la = row[66] or None
            starter.call_start_lb = row[67] or None
            starter.call_1_lb = row[68] or None
            starter.call_2_lb = row[69] or None
            starter.call_3_lb = row[70] or None
            starter.stretch_lb = row[71] or None
            starter.finish_lb = row[72] or None
            starter.call_start_margin = row[73] or None
            starter.call_1_margin = row[74] or None
            starter.call_2_margin = row[75] or None
            starter.call_3_margin = row[76] or None
            starter.stretch_margin = row[77] or None
            starter.finish_margin = row[78] or None
            starter.dh_flag = row[79] or None
            starter.horse_reg_id = row[80] or None
            starter.jockey_id = row[81] or None
            starter.trainer_id = row[82] or None
            starter.owner_id = row[83] or None
            starter.claimed_by_trainer_id = row[84] or None
            starter.claimed_by_owner_id = row[85] or None
            starter.eqb_ref_name = row[86] or None
            starter.void_ind = row[87] or None
            starter.void_reason = row[88] or None

            db.session.add(starter)

        db.session.commit()

    # DELETE UNZIPPED FILES IN TMP FOLDER
    folder = 'files/tmp'
    for f in os.listdir(folder):
        os.unlink(os.path.join(folder, f))

    print("{0} {1}: {2} races, {3} starters loaded.\n".format(
        track_code,
        race_date,
        r,
        s
    ))


@click.command()
@click.option('-y', '--year', prompt=True, default=None)
@with_appcontext
def load_ccf_year(year):
    """
    Load Brisnet chart files (ccf) for whole year by folder
    """
    folder = f'files/ccf/{year}'

    for f in os.listdir(folder):
        if not f.startswith('.'):
            cmd = 'flask load-ccf -f {0}'.format(
                os.path.join(folder, f)
            )
            subprocess.call(cmd, shell=True)
