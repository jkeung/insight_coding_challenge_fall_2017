# Table of Contents
1. [Instructions](README.md#instructions)
2. [Introduction](README.md#introduction)
3. [Challenge summary](README.md#challenge-summary)
4. [Details of challenge](README.md#details-of-challenge)
5. [Input file](README.md#input-file)
6. [Output files](README.md#output-files)

# Instructions

## Setup
You are encouraged to use a virtual environment using virtualenv and pip. There are currently no requirements because all the modules used in this solution are native.

```
$ virtualenv venv
$ source venv/bin/activate
```

## Dependencies

Python 2.7
 
- **heapq** - priority queue algorithm used to efficiently obtain root from an array
- **sys** - interpreter related and used to parse parameters from the commandline
- **time** - library to track run time of script

## Check Out and Run
This code can be run separately or from within the provided shell script.

```
$ git clone git@github.com:jkeung/insight_coding_challenge_fall_2017.git
$ cd insight_coding_challenge_fall_2017
$ python ./src/find_political_donors.py ./input/itcont.txt ./output/medianvals_by_zip.txt ./output/medianvals_by_date.txt
OR
$ ./run.sh
```
# Introduction
You’re a data engineer working for political consultants and you’ve been asked to help identify possible donors for a variety of upcoming election campaigns. 

The Federal Election Commission regularly publishes campaign contributions and while you don’t want to pull specific donors from those files — because using that information for fundraising or commercial purposes is illegal — you want to identify the areas (zip codes) that may be fertile ground for soliciting future donations for similar candidates. 

Because those donations may come from specific events (e.g., high-dollar fundraising dinners) but aren’t marked as such in the data, you also want to identify which time periods are particularly lucrative so that an analyst might later correlate them to specific fundraising events.

# Challenge summary

For this challenge, we're asking you to take an input file that lists campaign contributions by individual donors and distill it into two output files:

1. `medianvals_by_zip.txt`: contains a calculated running median, total dollar amount and total number of contributions by recipient and zip code

2. `medianvals_by_date.txt`: has the calculated median, total dollar amount and total number of contributions by recipient and date.

As part of the team working on the project, another developer has been placed in charge of building the graphical user interface, which consists of two dashboards. The first would show the zip codes that are particularly generous to a recipient over time while the second would display the days that were lucrative for each recipient. 

Your role on the project is to work on the data pipeline that will hand off the information to the front-end. As the backend data engineer, you do **not** need to display the data or work on the dashboard but you do need to provide the information.

You can assume there is another process that takes what is written to both files and sends it to the front-end. If we were building this pipeline in real life, we’d probably have another mechanism to send the output to the GUI rather than writing to a file. However for the purposes of grading this challenge, we just want you to write the output to files.



# Details of challenge

You’re given one input file, `itcont.txt`. Each line of the input file contains information about a campaign contribution that was made on a particular date from a donor to a political campaign, committee or other similar entity. Out of the many fields listed on the pipe-delimited line, you’re primarily interested in the zip code associated with the donor, amount contributed, date of the transaction and ID of the recipient.

Your code should process each line of the input file as if that record was sequentially streaming into your program. For each input file line, calculate the running median of contributions, total number of transactions and total amount of contributions streaming in so far for that recipient and zip code. The calculated fields should then be formatted into a pipe-delimited line and written to an output file named `medianvals_by_zip.txt` in the same order as the input line appeared in the input file. 

Your program also should write to a second output file named `medianvals_by_date.txt`. Each line of this second output file should list every unique combination of date and recipient from the input file and then the calculated total contributions and median contribution for that combination of date and recipient. 

The fields on each pipe-delimited line of `medianvals_by_date.txt` should be date, recipient, total number of transactions, total amount of contributions and median contribution. Unlike the first output file, this second output file should have lines sorted alphabetical by recipient and then chronologically by date.

Also, unlike the first output file, every line in the `medianvals_by_date.txt` file should be represented by a unique combination of day and recipient -- there should be no duplicates. 


## Input file

The Federal Election Commission provides data files stretching back years and is [regularly updated](http://classic.fec.gov/finance/disclosure/ftpdet.shtml)

For the purposes of this challenge, we’re interested in individual contributions. While you're welcome to run your program using the data files found at the FEC's website, you should not assume that we'll be testing your program on any of those data files or that the lines will be in the same order as what can be found in those files. Our test data files, however, will conform to the data dictionary [as described by the FEC](http://classic.fec.gov/finance/disclosure/metadata/DataDictionaryContributionsbyIndividuals.shtml).

Also, while there are many fields in the file that may be interesting, below are the ones that you’ll need to complete this challenge:

* `CMTE_ID`: identifies the flier, which for our purposes is the recipient of this contribution
* `ZIP_CODE`:  zip code of the contributor (we only want the first five digits/characters)
* `TRANSACTION_DT`: date of the transaction
* `TRANSACTION_AMT`: amount of the transaction
* `OTHER_ID`: a field that denotes whether contribution came from a person or an entity 

### Input file considerations

Here are some considerations to keep in mind:
1. Because we are only interested in individual contributions, we only want records that have the field, `OTHER_ID`, set to empty. If the `OTHER_ID` field contains any other value, ignore the entire record and don't include it in any calculation
2. If `TRANSACTION_DT` is an invalid date (e.g., empty, malformed), you should still take the record into consideration when outputting the results of `medianvals_by_zip.txt` but completely ignore the record when calculating values for `medianvals_by_date.txt`
3. While the data dictionary has the `ZIP_CODE` occupying nine characters, for the purposes of the challenge, we only consider the first five characters of the field as the zip code
4. If `ZIP_CODE` is an invalid zipcode (i.e., empty, fewer than five digits), you should still take the record into consideration when outputting the results of `medianvals_by_date.txt` but completely ignore the record when calculating values for `medianvals_by_zip.txt`
5. If any lines in the input file contains empty cells in the `CMTE_ID` or `TRANSACTION_AMT` fields, you should ignore and skip the record and not take it into consideration when making any calculations for the output files
6. Except for the considerations noted above with respect to `CMTE_ID`, `ZIP_CODE`, `TRANSACTION_DT`, `TRANSACTION_AMT`, `OTHER_ID`, data in any of the other fields (whether the data is valid, malformed, or empty) should not affect your processing. That is, as long as the four previously noted considerations apply, you should process the record as if it was a valid, newly arriving transaction. (For instance, campaigns sometimes retransmit transactions as amendments, however, for the purposes of this challenge, you can ignore that distinction and treat all of the lines as if they were new)
7. For the purposes of this challenge, you can assume the input file follows the data dictionary noted by the FEC for the 2015-current election years
8. The transactions noted in the input file are not in any particular order, and in fact, can be out of order chronologically

## Output files

For the two output files that your program will create, the fields on each line should be separated by a `|`

**`medianvals_by_zip.txt`**

The first output file `medianvals_by_zip.txt` should contain the same number of lines or records as the input data file minus any records that were ignored as a result of the 'Input file considerations.'

Each line of this file should contain these fields:
* recipient of the contribution (or `CMTE_ID` from the input file)
* 5-digit zip code of the contributor (or the first five characters of the `ZIP_CODE` field from the input file)
* running median of contributions received by recipient from the contributor's zip code streamed in so far. Median calculations should be rounded to the whole dollar (drop anything below $.50 and round anything from $.50 and up to the next dollar) 
* total number of transactions received by recipient from the contributor's zip code streamed in so far
* total amount of contributions received by recipient from the contributor's zip code streamed in so far

When creating this output file, you can choose to process the input data file line by line, in small batches or all at once depending on which method you believe to be the best given the challenge description. However, when calculating the running median, total number of transactions and total amount of contributions, you should only take into account the input data that has streamed in so far -- in other words, from the top of the input file to the current line. See the below example for more guidance.

**`medianvals_by_date.txt`**

Each line of this file should contain these fields:
* recipeint of the contribution (or `CMTE_ID` from the input file)
* date of the contribution (or `TRANSACTION_DT` from the input file)
* median of contributions received by recipient on that date. Median calculations should be rounded to the whole dollar (drop anything below $.50 and round anything from $.50 and up to the next dollar) 
* total number of transactions received by recipient on that date
* total amount of contributions received by recipient on that date

This second output file does not depend on the order of the input file, and in fact should be sorted alphabetical by recipient and then chronologically by date.

# Example

Suppose your input file contained only the following few lines. Note that the fields we are interested in are in **bold** below but will not be like that in the input file. There's also an extra new line between records below, but the input file won't have that.

> **C00629618**|N|TER|P|201701230300133512|15C|IND|PEREZ, JOHN A|LOS ANGELES|CA|**90017**|PRINCIPAL|DOUBLE NICKEL ADVISORS|**01032017**|**40**|**H6CA34245**|SA01251735122|1141239|||2012520171368850783

> **C00177436**|N|M2|P|201702039042410894|15|IND|DEEHAN, WILLIAM N|ALPHARETTA|GA|**300047357**|UNUM|SVP, SALES, CL|**01312017**|**384**||PR2283873845050|1147350||P/R DEDUCTION ($192.00 BI-WEEKLY)|4020820171370029337

> **C00384818**|N|M2|P|201702039042412112|15|IND|ABBOTT, JOSEPH|WOONSOCKET|RI|**028956146**|CVS HEALTH|VP, RETAIL PHARMACY OPS|**01122017**|**250**||2017020211435-887|1147467|||4020820171370030285

> **C00177436**|N|M2|P|201702039042410893|15|IND|SABOURIN, JAMES|LOOKOUT MOUNTAIN|GA|**307502818**|UNUM|SVP, CORPORATE COMMUNICATIONS|**01312017**|**230**||PR1890575345050|1147350||P/R DEDUCTION ($115.00 BI-WEEKLY)|4020820171370029335

> **C00177436**|N|M2|P|201702039042410895|15|IND|JEROME, CHRISTOPHER|FALMOUTH|ME|**041051896**|UNUM|EVP, GLOBAL SERVICES|**01312017**|**384**||PR2283905245050|1147350||P/R DEDUCTION ($192.00 BI-WEEKLY)|4020820171370029342

> **C00384818**|N|M2|P|201702039042412112|15|IND|BAKER, SCOTT|WOONSOCKET|RI|**028956146**|CVS HEALTH|EVP, HEAD OF RETAIL OPERATIONS|**01122017**|**333**||2017020211435-910|1147467|||4020820171370030287

> **C00177436**|N|M2|P|201702039042410894|15|IND|FOLEY, JOSEPH|FALMOUTH|ME|**041051935**|UNUM|SVP, CORP MKTG & PUBLIC RELAT.|**01312017**|**384**||PR2283904845050|1147350||P/R DEDUCTION ($192.00 BI-WEEKLY)|4020820171370029339

If we were to pick the relevant fields from each line, here is what we would record for each line.

    1.
    CMTE_ID: C00629618
    ZIP_CODE: 90017
    TRANSACTION_DT: 01032017
    TRANSACTION_AMT: 40
    OTHER_ID: H6CA34245

    2.
    CMTE_ID: C00177436
    ZIP_CODE: 30004
    TRANSACTION_DT: 01312017
    TRANSACTION_AMT: 384
    OTHER_ID: empty

    3. 
    CMTE_ID: C00384818
    ZIP_CODE: 02895
    TRANSACTION_DT: 01122017
    TRANSACTION_AMT: 250
    OTHER_ID: empty

    4.
    CMTE_ID: C00177436
    ZIP_CODE: 30750
    TRANSACTION_DT: 01312017
    TRANSACTION_AMT: 230
    OTHER_ID: empty

    5.
    CMTE_ID: C00177436
    ZIP_CODE: 04105
    TRANSACTION_DT: 01312017
    TRANSACTION_AMT: 384
    OTHER_ID: empty

    6.
    CMTE_ID: C00384818
    ZIP_CODE: 02895
    TRANSACTION_DT: 01122017
    TRANSACTION_AMT: 333
    OTHER_ID: empty

    7.
    CMTE_ID: C00177436
    ZIP_CODE: 04105
    TRANSACTION_DT: 01312017
    TRANSACTION_AMT: 384
    OTHER_ID: empty



We would ignore the first record because the `OTHER_ID` field contains data and is not empty. Moving to the next record, we would write out the first line of `medianvals_by_zip.txt` to be:

`C00177436|30004|384|1|384`

Note that because we have only seen one record streaming in for that recipient and zip code, the running median amount of contribution and total amount of contribution is `384`. 

Looking through the other lines, note that there are only two recipients for all of the records we're interested in our input file (minus the first line that was ignored due to non-null value of `OTHER_ID`). 

Also note that there are two records with the recipient `C00177436` and zip code of `04105` totaling $768 in contributions while the recipient `C00384818` and zip code `02895` has two contributions totaling $583 (250 + 333) and a median of $292 (583/2 = 291.5 or 292 when rounded up) 

Processing all of the input lines, the entire contents of `medianvals_by_zip.txt` would be:

    C00177436|30004|384|1|384
    C00384818|02895|250|1|250
    C00177436|30750|230|1|230
    C00177436|04105|384|1|384
    C00384818|02895|292|2|583
    C00177436|04105|384|2|768

If we drop the zip code, there are four records with the same recipient, `C00177436`, and date of `01312017`. Their total amount of contributions is $1,382. 

For the recipient, `C00384818`, there are two records with the date `01122017` and total contribution of $583 and median of $292.

As a result, `medianvals_by_date.txt` would contain these lines in this order:

    C00177436|01312017|384|4|1382
    C00384818|01122017|292|2|583

