import pandas as pd

pd.options.display.max_columns = None
pd.options.display.max_rows = None
pd.options.display.max_info_columns = 1000


def prepare(fn, plan):
    df = pd.read_csv(fn, keep_default_na=False, na_values='')
    df['region_name'] = df['region_name'].fillna('')
    df['date'] = pd.to_datetime(df['date'])
    df['plan'] = plan
    df['prediction_start_date'] = df.loc[df['predicted'] == True]['date'].min()
    columns_to_drop = []
    for column in df.columns:
        if str(column).find('_ma_3') > 0:
            columns_to_drop.append(column)
        if str(column).find('_ma_21') > 0:
            columns_to_drop.append(column)
        if str(column).find('--') > 0:
            columns_to_drop.append(column)
    df = df.drop(columns=columns_to_drop)
    df = df.drop(columns=['predicted_new_cases',
                          'confirmed_deaths',
                          'country_code3',
                          'country_code_numeric',
                          'day_of_year',
                          'month',
                          'quarter',
                          'week',
                          'working_day_ma_7',
                          'working_day_tomorrow',
                          'working_day_yesterday',
                          'year'])
    df['working_day'] = df['working_day'].apply(lambda x: True if x == 1.0 else False)
    df['stringency'] = (df['c1_school_closing'] / 3.) + \
                       (df['c2_workplace_closing'] / 3.) + \
                       (df['c3_cancel_public_events'] / 2.) + \
                       (df['c4_restrictions_on_gatherings'] / 4.) + \
                       (df['c5_close_public_transport'] / 2.) + \
                       (df['c6_stay_at_home_requirements'] / 3.) + \
                       (df['c7_restrictions_on_internal_movement'] / 2.) + \
                       (df['c8_international_travel_controls'] / 4.) + \
                       (df['h1_public_information_campaigns'] / 2.) + \
                       (df['h2_testing_policy'] / 3.) + \
                       (df['h3_contact_tracing'] / 2.) + \
                       (df['h6_facial_coverings'] / 4.)
    df.to_csv(f"processed{fn}", index=False)


prepare("_plan_0.csv", 'Intervention Plan 1')
prepare("_plan_1.csv", 'Intervention Plan 2')
prepare("_plan_2.csv", 'Intervention Plan 3')
prepare("_plan_3.csv", 'Intervention Plan 4')
prepare("_plan_4.csv", 'Intervention Plan 5')
prepare("_plan_5.csv", 'Intervention Plan 6')
prepare("_plan_6.csv", 'Intervention Plan 7')
prepare("_plan_7.csv", 'Intervention Plan 8')
prepare("_plan_8.csv", 'Intervention Plan 9')
prepare("_plan_9.csv", 'Intervention Plan 10')
prepare("_plan_baseline.csv", 'Baseline')
