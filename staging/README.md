Module responsible for loading SOCOTU raw input data into a desired format.
The package can load data into a database table or csv file.

How to run ?
* Install the required dependencies from requirements.txt. It is recommended to launch
the project in a separate environment using venv.


    python -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt

* Run the cli.py script and pass the working directory containing the raw txt files.


    python cli.py --work_dir=PATH_TO_DATA

As the default loading option is to the database, make sure to include the necessary
information in .env file. Ideally, the password should be set manually on the machine.
It is only included in the repo to simplify running the project on different machines.