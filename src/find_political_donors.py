from sys import argv
import time
import contributions
# Program that calculates the median number
# Author: Jason Keung
# Created: October 29, 2017


if __name__ == "__main__":
    script, infile, outfile_zip, outfile_date = argv
    start_time = time.time()
    print("Starting find_political_donors.py...")

    my_contribution = contributions.ContributionManager(
        infile, outfile_zip, outfile_date)

    # stream
    while my_contribution.read_contribution():

        my_contribution.create_contribution()
        my_contribution.update_running_date()

        write_zip_bool = my_contribution.update_running_zip_code()
        if write_zip_bool:
            my_contribution.write_stats('zip_code')

    # batch
    my_contribution.write_stats('date')

    my_contribution.close()

    print("find_political_donors.py run successfully!")
    print("--- %s seconds ---\n") % (time.time() - start_time)
