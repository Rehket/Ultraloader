# Ultra Loader

A command line tool for using the Bulk 2.0 API for SalesForce

-----------------------------------------
[![Run Tests](https://github.com/Rehket/Ultraloader/actions/workflows/tests.yml/badge.svg?branch=main)](https://github.com/Rehket/Ultraloader/actions/workflows/tests.yml)
[![Formatting Check](https://github.com/Rehket/Ultraloader/actions/workflows/black.yml/badge.svg?branch=main)](https://github.com/Rehket/Ultraloader/actions/workflows/black.yml)

Pre-Release - Do not expect much before this gets to version 1.0.0

## Commands

* `python ultra --help` - Show the help details.
* `ultra get-job [OPTIONS] JOB_ID` - Get the job details from SalesForce. Prints the output as formatted json.
* `ultra login` - Get a bearer token for the provided credentials. The credentials
  will be saved in the user directory under the .login

## Project layout

    env.sh    # The environment variables used to configure the tool.
    docs/
        index.md  # The documentation homepage.
        ...       # Other markdown pages, images and other files.
    ultra/    # The source code directory.
    tests/    # Test Directory.
