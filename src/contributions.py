from __future__ import division
import heapq as hq


class ContributionManager(object):
    """This class is created to read in contributions from an input file and also write median values by zip and date
    to an output file. It also contains creates statistics for contributors grouped by recipient/zip code as well 
    as recipient/date.

    Attributes:
        infile (str): Name of the file that contains the individual contributions based on the following data dictionary
                        (http://classic.fec.gov/finance/disclosure/metadata/DataDictionaryContributionsbyIndividuals.shtml)
        outfile_zip (str): Name of the outfile for the median values of recipients and zip code
        outfile_date (str): Name of the outfile for the median values of recipient and date
    """

    def __init__(self, infile, outfile_zip, outfile_date):

        self.reader = open(infile, 'r')
        self.writer_zip = open(outfile_zip, 'w')
        self.writer_date = open(outfile_date, 'w')
        self.row = None
        self.contribution = None
        self.stats = {
            'zip_code': StatsByZip(),
            'date': StatsByDate()
        }

    def read_contribution(self):
        """Reads in contributions only if there is a non blank line in the file.
        """

        self.row = self.reader.readline()

        if len(self.row) > 0:
            return True
        else:
            return False

    def write_stats(self, groupby):
        """Write contributions to output file based on the type of statistic collected.

        Args:
            groupby (str): The grouping for recipients based on zip code or date. Possible values are 'zip_code' or 'date'
        """

        if groupby == 'zip_code':
            self.writer_zip.write(self.stats['zip_code'].write(self.contribution))

        if groupby == 'date':
            for line in self.stats['date'].write(self.contribution):
                self.writer_date.write(line)

    def create_contribution(self):
        """Creates a contribution object based on the '|' delimited input file format and performs business logic.
        """

        row_list = self.row.split('|')
        self.contribution = Contribution(row_list)
        self.contribution.parse_contribution()

    def update_running_zip_code(self):
        """Adds the current contribution amount to each statistic being captured and updates the running median value.
        """

        # omit blank other_id
        if self.contribution.other_id != '':
            pass
        # omit entire record if cmte_id or transaction amount missing
        elif self.contribution.cmte_id == '' or self.contribution.transaction_amt == '':
            pass
        # omit from zip code if zip code invalid
        elif len(self.contribution.zip_code) < 5:
            pass
        else:
            self.stats['zip_code'].add(self.contribution)
            return True

        return False

    def update_running_date(self):
        """Adds the current contribution amount to each statistic being captured and updates the running median value.
        """

        # omit blank other_id
        if self.contribution.other_id != '':    
            pass
        # omit entire record if cmte_id or transaction amount missing
        elif self.contribution.cmte_id == '' or self.contribution.transaction_amt == '':    
            pass
        # omit if date is not length 8
        elif len(self.contribution.transaction_dt) != 8:   
            pass
        else:
            self.stats['date'].add(self.contribution)

    def close(self):
        """Function to close all statistic writers.
        """
        self.writer_zip.close()
        self.writer_date.close()


class Stats(object):
    """This is a base class to capture a statistic based on a flexible aggregation.
    """

    def __init__(self):
        self.data = {}

    def _get_group(self, contribution):
        raise NotImplemented

    def add(self, contribution):
        """ Adds a new contribution to the existing contribution information

        Args:
            contribution(Contribution): A new contribution
        """
        cmte_id = contribution.cmte_id
        amnt = contribution.transaction_amt
        group = self._get_group(contribution)

        # adds to data if this is a first time recipient
        if cmte_id not in self.data:
            self.data[cmte_id] = {}

        # adds to data if this is the first time for this grouping
        if group not in self.data[cmte_id]:
            self.data[cmte_id][group] = RunningMedian()

        self.data[cmte_id][group].add(amnt)

    def write(self, contribution):
        """ Writes the contribution information

        Args:
            contribution(Contribution): A new contribution
        """
        raise NotImplemented


class StatsByZip(Stats):
    """Contribution for each recipient based these fields:

        - recipient of the contribution (or CMTE_ID from the input file)
        - 5-digit zip code of the contributor (or the first five characters of the ZIP_CODE field from the input file)
        - running median of contributions received by recipient from the contributor's zip code streamed in so far. 
            Median calculations should be rounded to the whole dollar (drop anything below $.50 and round anything from $.50 and up to the next dollar)
        - total number of transactions received by recipient from the contributor's zip code streamed in so far
        - total amount of contributions received by recipient from the contributor's zip code streamed in so far
    """

    def __init__(self):
        super(StatsByZip, self).__init__()

    def _get_group(self, contribution):
        return contribution.zip_code

    def write(self, contribution):
        """Function to return a '|' delimited string which contains the updated contribution information.

        Args:
            contribution(Contribution): A new contribution

        Returns:
            str: A '|' delimited string which contains the contribution information based on recipient and zip code.
        """
        cmte_id = contribution.cmte_id
        group = self._get_group(contribution)
        amnt = str(int(round(self.data[cmte_id][group].median)))
        cnt = str(int(round(self.data[cmte_id][group].count)))
        total = str(int(round(self.data[cmte_id][group].total)))
        return '|'.join([cmte_id, group, amnt, cnt, total]) + '\n'


class StatsByDate(Stats):
    """Contribution for each recipient based these fields:

        - recipeint of the contribution (or CMTE_ID from the input file)
        - date of the contribution (or TRANSACTION_DT from the input file)
        - median of contributions received by recipient on that date. 
            Median calculations should be rounded to the whole dollar (drop anything below $.50 and round anything from $.50 and up to the next dollar)
        - total number of transactions received by recipient on that date
        - total amount of contributions received by recipient on that date
    """

    def __init__(self):
        super(StatsByDate, self).__init__()

    def _get_group(self, contribution):
        return contribution.transaction_dt

    def write(self, contribution):
        """Function to generate a '|' delimited string which contains the updated contribution information. 

        Args:
            contribution(Contribution): A new contribution

        Yields:
            str: A '|' delimited string which contains the contribution information based on recipient and date.
        """
        for cmte_id, date_running_median in sorted(self.data.items()):
            for dt, running_median in sorted(date_running_median.items()):
                amnt = str(int(round(running_median.median)))
                cnt = str(int(round(running_median.count)))
                total = str(int(round(running_median.total)))

                yield '|'.join([cmte_id, dt, amnt, cnt, total]) + '\n'


class Contribution(object):
    """Contains transformation of a row to individual contribution elements.
    """

    def __init__(self, row_list):
        self.row_list = row_list
        self.cmte_id = None
        self.zip_code = None
        self.transaction_dt = None
        self.transaction_amt = None
        self.other_id = None

    def parse_contribution(self):
        self.cmte_id = self.row_list[0]
        self.zip_code = self.row_list[10][:5] # Only keep first 5 characters
        self.transaction_dt = self.row_list[13]
        self.transaction_amt = float(self.row_list[14])
        self.other_id = self.row_list[15]


class RunningMedian(object):
    """Defines a class for RunningMedian
        Attributes:
            minheap (list): The min heap that will contain the higher numbers.
            maxheap (list): The max heap that will contain the lower numbers.
            even (boolean): Condition that will specify which heap has more
                elements. 
    """

    def __init__(self):
        self.minheap = []
        self.maxheap = []
        self.even = True
        self.median = 0
        self.count = 0
        self.total = 0

    def add(self, val):
        """Method to add a running median to minheap or maxheap. 
        A maxheap will be represented by negative numbers. 
        The following algorithm will be used:
        Step 1: Add next item to one of the heaps
        if next item is smaller than maxHeap root add it to maxHeap,
        else add it to minHeap
        Step 2: Balance the heaps (after this step heaps will be either
        balanced or one of them will contain 1 more item)
        if number of elements in one of the heaps is greater than the other by
        more than 1, remove the root element from the one containing more
        elements and add to the other one
        (http://stackoverflow.com/questions/10657503/find-running-median-from-a-stream-of-integers)
        Args:
            val (int): Median to be added to the heap.
        """

        if self.even:
            # to intiailize if maxheap is empty
            if len(self.maxheap) == 0:
                hq.heappush(self.maxheap, val * -1)
            # if value is less than root of maxheap push to maxheap
            elif val < self.maxheap[0] * -1:
                hq.heappush(self.maxheap, val * -1)
            # push to minheap and then force maxheap to have more elements
            else:
                temp = hq.heappushpop(self.minheap, val)
                hq.heappush(self.maxheap, temp * -1)
            # set even condition to False, maxheap has 1 additional element
            self.even = False

        else:
            # push to maxheap then balance to minheap
            if val < self.maxheap[0] * -1:
                temp = hq.heappushpop(self.maxheap, val * -1)
                hq.heappush(self.minheap, temp * -1)
            # push to minheap
            else:
                hq.heappush(self.minheap, val)
            # maxheap has same number elements as minheap
            self.even = True
        # only keep 3 values stored in the heap for memory purposes
        self.maxheap = hq.nsmallest(3, self.maxheap)
        self.minheap = hq.nsmallest(3, self.minheap)

        self.count += 1
        self.total += val

        if self.even:
            self.median = (min(self.maxheap) * -1 + min(self.minheap)) / 2.
        else:
            self.median = min(self.maxheap) * -1
