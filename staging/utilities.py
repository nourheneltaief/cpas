def extract_date(line_date: str):
    return line_date[2: 4] + '/' + line_date[4: 6] + '/' + line_date[:2]

def get_debit_sold_credit(sens: str, amount: float):
    if sens == 'D':
        return amount, amount, 0.0
    else:
        return 0.0, -amount, amount

def get_lines_info(lines: list[str]):
    rows = []
    for line in lines:
        sens = line[71: 72]
        amount = float(line[105:])
        debit, sold, credit = get_debit_sold_credit(sens, amount)
        rows.append((
            # aux       folio         num_comp      pc_line       date                        journal_code  code_agence
            line[8:16], line[16: 18], line[18: 23], line[23: 25], extract_date(line[25: 31]), line[31: 33], line[33: 36],
            # libelle     sens  analytical    jr_code_1             user          cg
            line[36: 71], sens, line[72: 76], line[76: 94].strip(), line[94: 96], line[96: 105], amount, debit, sold,
            credit
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