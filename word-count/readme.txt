wordcount(Per Task Tally) - is the per task tally implementation of word count, such that word count is calculated at map task level.
si combiner - a combiner is used to combine the word count after each map task.
no combiner - no combiner is used hence no precombining of word count takes place at the mapper.
per map tally - word count is calculated locally at each mapper.
