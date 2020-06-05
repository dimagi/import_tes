Import TEIs
===========

`import_teis.py` is an example script to demonstrate bulk-importing
DHIS2 Tracked Entity Instances as CommCare cases.

Requires Python 3.5 or higher.

### Usage

1. Install requirements:

        $ pip install -r requirements.txt

2. Customize constants at the start of the script, and customize the
   `get_name()` function, if necessary:

        $ $EDITOR import_teis.py

3. Run:

        $ python3 import_teis.py


### Improvements

This script stores usernames and passwords in constants. This is to keep
the script simple for the sake of illustration. It should not be done in
practice. Rather use environment variables if the script is to be
deployed, or prompt the user for credentials if it is used from the
command line. (Do not pass passwords in command line options.)
