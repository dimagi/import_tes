Import TEIs
===========

`import_teis.py` is an example script to demonstrate bulk-importing
DHIS2 Tracked Entity Instances as CommCare cases.

Requires Python 3.5 or higher.

### Usage

1. Clone the repository and install Python requirements:

        $ git clone https://github.com/dimagi/import_tes.git
        $ cd import_tes
        $ pip install -r requirements.txt

2. Configure the constants at the start of the script with your
   settings, and customize the `get_name()` function if necessary:

        $ nano import_teis.py

3. Run:

        $ python3 import_teis.py


### Improvements

This script stores usernames and passwords in constants. This is to keep
the script simple for the sake of illustration. It should not be done in
practice. Rather use environment variables if the script is to be
deployed, or prompt the user for credentials if it is used from the
command line. (Do not pass passwords in command line options.)
