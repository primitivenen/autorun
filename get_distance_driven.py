import numpy as np
from COL_DEPENDENCY_DICT import *


def get_distance_driven(df, option='dist', jump_threshold=10):
    """
    Given a Dataframe of odometer readings, calculate distance driven in
    total, and/or odometer offset

    Parameters
    ----------
    df: pandas DataFrame, required
        one column assumed to be timestamp, another column is odometer reading
    option: str, default 'dist'
        - if 'dist': get distance driven
        - if 'offset': get mileage offset values
        - if 'all': return (dist_driven, offset) tuple
          odometer offset is defined as jumps of odometer readings from time to
          time, the jump could be negative or positive
    jump_threshold: int, default 10, optional
        the threshold value to decide whether a mileage jump is valid.
        any mileage difference greater than this threshold is deemed as a jump

    Returns
    -------
    distance_driven: total distance driven, in the same unit of odometer
    OR
    offset: odometer offset values
    """
    df = df.dropna(subset=COL_DEPENDENCY_DICT['distance_driven'])
    df = df[df['ICM_TOTALODOMETER'] > 0]  # drop zero

    odometers = df.loc[:, 'ICM_TOTALODOMETER']
    if odometers.empty:
        dist_driven, offset = np.nan, np.nan
    else:
        odometer_diffs = odometers.diff().dropna()

        # get total distance
        condition = odometer_diffs.abs() < jump_threshold
        dist_driven = odometer_diffs[condition].sum()
        dist_driven_sign = float(np.sign(dist_driven))

        # get offset
        offset_vals = odometer_diffs[~condition]
        offset = offset_vals.sum() * dist_driven_sign

    if option == 'dist':
        return abs(dist_driven)
    elif option == 'offset':
        return offset
    elif option == 'all':
        return abs(dist_driven), offset


def get_dist_on_battery(df, min_soc=20):
    """
    Compute distance driven on battery only. Consider condition of (positive ICM_TOTALODOMETER,
    zero or null CCS_CHARGECUR, zero or null HCU_BATCHRGDSP) as driving, then based on this,
    consider condition of (zero HCU_AVGFUELCONSUMP, minimum SOC is min_soc) as driving on battery
    only.

    Parameters
    ----------
    df: pandas DataFrame object, required
    min_soc: int, default 20
        min soc under which the veh will not be able to drive on battery at all

    Returns
    -------
    dist_batt: distance driven on battery
    """
    dist = 0
    if df.empty:
        dist = np.nan
    else:
        filter1 = df['ICM_TOTALODOMETER'] > 0
        filter2 = (df['CCS_CHARGECUR'] < 0.1) | (df['CCS_CHARGECUR'].isna())
        filter3 = (df['HCU_BATCHRGDSP'] == 0) | (df['HCU_BATCHRGDSP'].isna())
        filter_driving = filter1 & filter2 & filter3

        filter4 = df['HCU_AVGFUELCONSUMP'] < 0.000001
        filter5 = df['BMS_BATTSOC'] > min_soc

        filter_drive_batt = filter4 & filter5
        # flag = 1 means driving on battery
        df['flag'] = (filter_driving & filter_drive_batt).astype(int)
        df = df[df['flag'] == 1].reset_index()  # original index is kept
        df.loc[:, 'index'] = df['index'].diff().fillna(2.0)

        # indices keep track of starting point of segment of driving_on_batt
        indices = df.index[df['index'] != 1.0].tolist()
        if not df.empty and df['index'].iloc[-1] == 1.0:
            indices.append(df.index[-1])

        for i in range(len(indices[:-1])):
            start = indices[i]
            end = min(indices[i + 1] + 1, df.shape[0])
            # take segment of dataframe, on which the veh is driving on battery only
            df_seg = df.iloc[start:end, :].loc[:, ['TDATE', 'ICM_TOTALODOMETER']]
            dist_seg = get_distance_driven(df_seg, 'dist')
            if not np.isnan(dist_seg):
                dist += dist_seg
    return dist
