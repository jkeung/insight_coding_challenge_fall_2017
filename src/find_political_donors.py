from sys import argv    
import time             
import contributions           
# Program that calculates the median number of unique words per tweet
# Author: Jason Keung
# Created: October 29, 2017


if __name__ == "__main__":
    script, infile, outfile_zip, outfile_date = argv
    start_time = time.time()
    print ("Starting median_unique.py...")
    
    my_contribution = contributions.ContributionManager(infile, outfile_zip, outfile_date)
    
    # stream
    while my_contribution.read_contribution():

        my_contribution.create_contribution()
        
        if my_contribution.contribution.other_id == '':
            my_contribution.update_running_median()
            my_contribution.write_stats('zip_code')
            
        else:
            pass
    
    # batch    
    my_contribution.write_stats('date')

    my_contribution.close()

    # print ("Output is saved to %s ") % (outfile)
    print ("median_unique.py run successfully!")
    print ("--- %s seconds ---\n") % (time.time() - start_time)