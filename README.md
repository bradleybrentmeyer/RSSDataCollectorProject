# RSSDataCollectorProject
https://github.com/

The subset of sentiment labeled (positive/negative) financial news headlines collected by this process can be found in file newsHeadlinesWithCorpNamesAndSentimentLabels.  The larger unlabeled file, not included here consists of 136,069 distinct financial news headlines collected from multiple sources such as, Bloomberg, Reuters, Market Watch, Yahoo Finance, etc. If you wish to use this larger dataset please contact me via email. 

Sentiment labeling was conditioned on the assumption that the sentiment labeler was an investor and held a long equity position in the company mentioned within the financial news headline. Thus if after reading the headline an informed investor would reasonably expect the value of company’s stock price to rise the headline would be labeled as positive, and con-
versely if they reasonably expect the company’s stock price to fall it would be labeled negative. The overall sentiment of the headline was considered, that is each word in the sentence is interpreted in the context of the entire sentence, words are not independent. Each headline is however considered independent of all other headlines.  Following standard convention and to facilitate computation positive sentiment news headlines were labeled +1 and negative sentiment headlines were labeled -1. News headlines which did not emote sentiment were evaluated as neutral and not included in our labeled data set. The labeling was performed by a financial industry practitioner. In total 5200 news headlines were labeled covering 397 companies in the S&P 500. 

The sentiment study which uses this data can be found at:http://ieeexplore.ieee.org/document/7925378/
