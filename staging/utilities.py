from datetime import datetime

def extract_date(line_date: str):
    month = line_date[2: 4]
    day = line_date[4: 6]
    year = line_date[:2]
    return month, day, year

def is_valid_date(month, day, year):
    date_str = f"{month}/{day}/{year}"
    try:
        d = datetime.strptime(date_str, "%m/%d/%y")
        # do not extract rows when the date is first or last of year
        return True and d.timetuple().tm_yday not in [1, 365, 364]
    except ValueError:
        return False

def get_debit_sold_credit(sens: str, amount: float):
    if sens == 'D':
        return amount, amount, 0.0
    else:
        return 0.0, -amount, amount

def get_lines_info(lines: list[str]):
    rows = []
    for line in lines:
        aux = line[8:16]
        month, day, year = extract_date(line[25: 31])
        if len(aux.strip()) not in [0, 4] or not is_valid_date(month, day, year):
            continue

        sens = line[71: 72]
        amount = float(line[105:])
        debit, sold, credit = get_debit_sold_credit(sens, amount)
        rows.append((
            aux if len(aux.strip()) != 0 else None, line[16: 18], line[18: 23], line[23: 25], f"{month}/{day}/{year}",
            line[31: 33], line[33: 36], line[36: 71], sens, line[72: 76], line[76: 94].strip(), line[94: 96],
            line[96: 105], amount, debit, sold, credit
        ))
    return rows

def drop_table_if_exists_query(table_name:str):
    return f"DROP TABLE IF EXISTS {table_name};"

def create_table_query(table_name:str):
    return f""" CREATE TABLE {table_name} (
        auxiliaire TEXT,
        folio TEXT,
        numero_piece_comptable TEXT,
        ligne_pc TEXT,
        date_pc TEXT,
        code_journal TEXT,
        agence TEXT,
        libelle TEXT,
        sens TEXT,
        code_journal_1 TEXT,
        code_analytique TEXT,
        utilisateur TEXT,
        cg TEXT,
        montant FLOAT,
        debit FLOAT,
        credit FLOAT,
        solde FLOAT
    );
    
    """

def insert_all_rows(table_name:str):
    return f"""
        INSERT INTO {table_name} (
            auxiliaire, folio, numero_piece_comptable, ligne_pc, date_pc,
            code_journal, agence, libelle, sens, code_journal_1,
            code_analytique, utilisateur, cg, montant, debit, credit, solde
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

def remove_unnecessary_labels(table_name: str):
    f"""
    Delete from {table_name}
    where libelle not like '%ARRETE%'
"""