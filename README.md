# 📊 Customer Profitability Analysis (CPA) - SOCOTU
A Python-based analytics tool for assessing and forecasting customer profitability in
an import-export company (SOCOTU). The system enables finance and strategy teams to identify
high-value customers, calculate profit margins, and make data-driven decisions.

### How to run ?
* Create a virtual environment to install the dependencies and run the application. Make sure to open
linux-like terminal in the root repo folder (..\cpas>) and run the following

```
    python -m venv venv
    . venv/Scripts/activate (if you are running a Windows machine, else: source venv/bin/activate)
    pip install -r requirements.txt  
```

* The orchestrator of the application is the main module. You need to run it, after identifying the
following
  * working directory: containing the raw_data folder (example = C:/Users/hp/Downloads)
  * configuration path: path to the .yml file containing the parameters to run the application.
* Once known, run the following:

```
    python -m main.cli --work_dir=PATH_TO_WORK_DIR --config=PATH_TO_CONFIG_FILE
```

* For example

```
    python -m main.cli --work_dir='C:/Users/hp/Downloads' --config='C:/Users/hp/Downloads/main_config.yml'
```

If you wish to load to the database, make sure to include the necessary information in main/.env 
file. Ideally, the password should be set manually on the machine. It is only included in the repo to
simplify running the project quickly on different machines.