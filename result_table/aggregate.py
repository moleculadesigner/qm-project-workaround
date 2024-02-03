"""
Script for reading Excel data and aggregate it to single csv file

Usage:

conda env create -f ../environment.yml
conda activate quantchem
python aggregate.py 'Excel Name.xlsx' 'n_atoms_list'
"""


import pandas as pd
import math
import sys

from pathlib import Path


def clean_multiindex(multiindex: tuple) -> tuple:
    return tuple(
        index if not index.startswith("Unnamed: ") else ""
        for index in multiindex
    )

def assign_missing_properties(df: pd.DataFrame):
    # current context
    mol_no, mol_name, brutto, owner = [None] * 4
    drop_index = []
    for idx, row in df.iterrows():
        if math.isnan(row[""][""][""]["Fundamental [cm-1]"]):
            drop_index.append(idx)
            continue
        if not math.isnan(row[""][""][""]["#"]):
            mol_no, mol_name, brutto, _, _, owner, *_ = row
        else:
            row[[
                ["", "", "", "#"],
                ["", "", "", "Molecule"],
                ["", "", "", 'Brutto'],
                ["", "", "", "Who calculated"],
            ]] = mol_no, mol_name, brutto, owner
        df.loc[idx] = row
    df.drop(
        labels=drop_index,
        axis=0,
        inplace=True,
    )

def process_excel_sheet(path: Path, n_atoms: int) -> pd.DataFrame:
    raw_data = pd.read_excel(
        path,
        sheet_name=f"{n_atoms}atoms",
        header=(0, 1, 2, 3),
    )

    # Drop empy column and add n_atoms
    raw_data.drop(raw_data.columns[6], axis=1, inplace=True)
    raw_data[("", "", "", "N atoms")] = n_atoms

    # Properly rename index
    cleaned_multiindex = pd.MultiIndex.from_tuples(
        [
            (clean_multiindex(idx))
            for idx in raw_data.columns
        ],
        names=('Method', 'Dispersion', 'Basis', 'Property')
    )

    clean_index_data = pd.DataFrame(columns=cleaned_multiindex)
    for src_col, dst_col in zip(raw_data.columns, clean_index_data.columns):
        clean_index_data[dst_col] = raw_data[src_col]

    # Add mol_no, brutto and owner to all rows
    assign_missing_properties(clean_index_data)
    return clean_index_data

def main():
    excel_path, n_atmos_path = map(Path, sys.argv[1:3])
    with n_atmos_path.open("r") as n_atoms_f:
        n_atoms_list = map(
            int,
            n_atoms_f.read().strip("\n").strip().split(),
        )

    aggregated_data = pd.concat(
        process_excel_sheet(excel_path, n_atoms)
        for n_atoms in n_atoms_list
    )

    aggregated_path = excel_path.parent.resolve().absolute() / f"{excel_path.stem}.csv"
    aggregated_data.to_csv(aggregated_path, index=False)

if __name__ == "__main__":
    main()