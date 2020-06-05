import_tes
==========

`import_tes.py` is an example script to demonstrate bulk-importing DHIS2
tracked entities as CommCare cases.

Requires Python 3.5 or higher.

### Usage

1. Install requirements:

        $ pip install -r requirements.txt

2. Customize constants at the start of the script, and customize the
   `get_name()` function, if necessary:

        $ $EDITOR import_tes.py

3. Run:

        $ python3 import_tes.py
