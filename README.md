# 📊 Customer Profitability Analysis (CPA) - SOCOTU
A Python-based analytics tool for assessing and forecasting customer profitability in
an import-export company. The system enables finance and strategy teams to identify
high-value customers, calculate profit margins, and make data-driven decisions.

### How to run ?
* Install the required dependencies from requirements.txt. It is recommended to launch
  the project in a separate environment using venv.

```
    python -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
```

* Run the cli.py script under the staging module and pass the working directory containing the 
raw txt files.

```
    python staging/cli.py --work_dir=PATH_TO_DATA
```

As the default loading option is to the database, make sure to include the necessary
information in staging/.env file. Ideally, the password should be set manually on the machine.
It is only included in the repo to simplify running the project quickly on different
machines.