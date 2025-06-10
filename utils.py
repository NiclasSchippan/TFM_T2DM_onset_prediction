import pandas as pd
import numpy as np

def add_latest_entry_before_date(goal_df, data_df, source_date_col, reference_date_col,
                                  value_col, result_col_name=None, id_col='idp'):
    """
    Adds the latest valid (non-negative if numeric) entry from data_df (before reference date in goal_df) to goal_df.

    Parameters:
    - goal_df (pd.DataFrame): Target dataframe containing the reference date column.
    - data_df (pd.DataFrame): Source dataframe with date and value columns.
    - source_date_col (str): Date column in data_df (format 'YYYYMMDD' as string).
    - reference_date_col (str): Column in goal_df to compare against.
    - value_col (str): Column name of the value to merge (e.g. 'smoking_status').
    - result_col_name (str or None): If provided, renames value_col in output to this.
    - id_col (str): Identifier column (default 'idp').

    Returns:
    - pd.DataFrame: Updated goal_df with new value column added.
    """
    # Use original value_col name if no rename requested
    if result_col_name is None:
        result_col_name = value_col

    # Copy source data and ensure date is datetime
    data_df = data_df.copy()
    data_df[source_date_col] = pd.to_datetime(data_df[source_date_col].astype(str), format='%Y%m%d')

    # Merge with goal_df
    help_df = goal_df.merge(data_df[[id_col, source_date_col, value_col]], on=id_col, how='left')

    # Define temp merged column names
    source_date_tmp = f'{source_date_col}_y'
    ref_date_tmp = f'{reference_date_col}_x'

    # Filter: only values before the reference date
    valid_entries = help_df[help_df[source_date_tmp] < help_df[ref_date_tmp]]

    # Select latest value per ID and ref date
    latest_entries = valid_entries.loc[
        valid_entries.groupby([id_col, ref_date_tmp])[source_date_tmp].idxmax()
    ]

    # Rename columns for merge
    latest_entries = latest_entries.rename(columns={
        ref_date_tmp: 'ref_date',
        value_col: result_col_name
    })

    # Check if values are numeric — if so, exclude negatives
    if pd.api.types.is_numeric_dtype(latest_entries[result_col_name]):
        before = len(latest_entries)
        latest_entries = latest_entries[latest_entries[result_col_name] >= 0]
        after = len(latest_entries)
        print(f"Filtered out {before - after} negative values in '{result_col_name}'.")

    # Final merge
    updated_df = goal_df.merge(
        latest_entries[[id_col, 'ref_date', result_col_name]],
        left_on=[id_col, reference_date_col],
        right_on=[id_col, 'ref_date'],
        how='left'
    ).drop(columns=['ref_date'])

    # Print NA count
    na_count = updated_df[result_col_name].isna().sum()
    print(f"Number of missing values in '{result_col_name}':", na_count)

    return updated_df


    

def add_latest_diagnosis_entry(goal_df, diagnostics_df, code_col, date_col,
                                fixed_codes=None, code_roots=None,
                                reference_date_col='t0',
                                result_col_name='diagnosis',
                                id_col='idp'):
    """
    Flags latest ICD diagnosis entry before reference date and merges it into the goal dataframe.

    Parameters:
    - goal_df (pd.DataFrame): Target dataframe (with 'idp' and reference date).
    - diagnostics_df (pd.DataFrame): DataFrame with diagnosis codes and dates.
    - code_col (str): Column name in diagnostics_df containing ICD codes.
    - date_col (str): Column name in diagnostics_df with diagnosis dates (YYYYMMDD as string).
    - fixed_codes (list of str): Exact ICD codes to match (e.g., ['I10']).
    - code_roots (list of str): Code roots to match any subcodes (e.g., ['I15'] → matches I15.0, I15.1...).
    - reference_date_col (str): Name of the date column in goal_df (e.g., 't0').
    - result_col_name (str): New column name in goal_df indicating diagnosis presence.
    - id_col (str): Patient ID column (default: 'idp').

    Returns:
    - pd.DataFrame: goal_df with the new diagnosis column merged.
    """

    # Convert diagnosis dates to datetime
    diagnostics_df = diagnostics_df.copy()
    diagnostics_df[date_col] = pd.to_datetime(diagnostics_df[date_col].astype(str), format='%Y%m%d')

    # Build mask from fixed codes and root-based regex
    mask = pd.Series(False, index=diagnostics_df.index)

    if fixed_codes:
        mask |= diagnostics_df[code_col].isin(fixed_codes)

    if code_roots:
        for root in code_roots:
            pattern = fr'^{root}(?:\.\d+)?$'  # matches root with or without .digits
            mask |= diagnostics_df[code_col].str.contains(pattern, regex=True, na=False)



    # Subset and normalize values
    matched_df = diagnostics_df[mask].copy()
    matched_df.loc[:, code_col] = '1'

    # Merge with goal_df to filter by date
    help_df = goal_df.merge(matched_df[[id_col, code_col, date_col]], on=id_col, how='left')

    # Identify merged column names
    source_date_tmp = f'{date_col}_y'
    ref_date_tmp = f'{reference_date_col}_x'

    # Filter only entries before reference date
    valid_entries = help_df[help_df[source_date_tmp] < help_df[ref_date_tmp]]

    # Select the latest entry per ID and reference date
    latest_entries = valid_entries.loc[
        valid_entries.groupby([id_col, ref_date_tmp])[source_date_tmp].idxmax()
    ]

    latest_entries = latest_entries.rename(columns={ref_date_tmp: 'ref_date', code_col: result_col_name})

    # Final merge with goal_df
    updated_df = goal_df.merge(
        latest_entries[[id_col, 'ref_date', result_col_name]],
        left_on=[id_col, reference_date_col],
        right_on=[id_col, 'ref_date'],
        how='left'
    ).drop(columns=['ref_date'])

    # Replace empty strings with NaN
    updated_df[result_col_name] = updated_df[result_col_name].replace(r'^\s*$', np.nan, regex=True)

    # Report missing values
    na_count = updated_df[result_col_name].isna().sum()
    print(f"Number of missing values in '{result_col_name}':", na_count)

    return updated_df

