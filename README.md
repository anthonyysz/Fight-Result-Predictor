# Fight-Result-Predictor

## Project Layout

- `backend/`
  - Historical and upcoming fight scrapers
  - Model backtesting, training and selection
  - All data, generated or referenced
  - FastAPI functions to scrape, load, predict, and resolve data
- `frontend/`
  - Front end react app
  - API connection to the RDS database
- `models/`
  - Most recent model versions
- `notebooks/`
  - Exploration, preprocessing, and modeling notebooks

## 1. Introduction
&nbsp;&nbsp;&nbsp;&nbsp;For my final project as a WGU student, I wanted to build something to really tie together a lot of the concepts I've learned during my time here. There are many concepts that we touched upon at WGU which I'd like to expand on. I want this project to tie together front-end and back-end programming, machine learning, and a CI/CD pipeline.

### 1a. My Idea
&nbsp;&nbsp;&nbsp;&nbsp;I wanted to build a machine learning model with measurable results and real-world effects that could be continuously trained and used. Sports betting is not something I'm particularly interested in, but sports-watching is. Combat sports are my favorite, but they can see highly varying results. While odds-makers can often predict the fight very well, age, style, momentum, and history can also play into things. I want to build a model that can take into account all aspects of a fight and how each fighter has faired previously in similar circumstances. 

### 1b. My Plan
&nbsp;&nbsp;&nbsp;&nbsp;My model will predict the results of a fight and return a level of confidence, determining how much the bettor should place on the fight and which fighter they should bet on. This confidence level will account for betting lines and upset likelihood, so fights that appear to be more one-sided will see higher bets. My goal is for the model to be profitable.

## 2. Data Entry and Cleaning
&nbsp;&nbsp;&nbsp;&nbsp;During this step, I had to manually enter some data after finding out that the dataset I used didn't properly scrape the data for every fight. Since there were around 600 rows with missing fight odds, I merged the imported dataset with some odds I found online. I also only kept columns that felt relevant to me.

&nbsp;&nbsp;&nbsp;&nbsp;Next, I had to do some house cleaning. There were 3 fighters without stance information and a fighter without their reach information. I also had to get some data types in order, including datetime for the date column, some integers showing up as floats, and changing some categorical data into boolean. I also created my difference columns here. 

&nbsp;&nbsp;&nbsp;&nbsp;Down at the bottom, I did some validation testing with values I thought might be a little bit tricky for the data scraper. The data scraper passed the test with the values it did bring to us, so that means we may not need to change to much when we run it ourselves. Once everything looked good, I saved the initial fights as testing.csv.

## 3. EDA, Preprocessing, and Backtesting

### 3a. EDA

&nbsp;&nbsp;&nbsp;&nbsp;Through exploratory data analysis, there weren't many patterns or situations where you could always find a profit margin in the fight game. The sportsbooks keep odds separated well enough to ensure that they always win. There were very few situations that I found which could give somewhere between a 1% and 5% return if you stuck with it, for example betting on the red corner fighter whenever they're on a winning streak. 

### 3b. Preprocessing

&nbsp;&nbsp;&nbsp;&nbsp;Outside of the standard Logistic Regression for a binary classification problem, I also chose to test Random Forest Classifier, Extra Random Trees Classifier, and a few boosting classifiers (XtremeGradientBoosting, GradientBoosting, and Adaptive Boosting). There were a few different parameters I wanted to try out for each of these, so I created a dictionary of parameters for each classifier. I also decided at this time that I would separate the models by weight class since fights between fighters of different sizes tend to play out differently. Finally, after applying my OneHotEncoder and StandardScaler, my data was ready for backtesting. 

### 3c. Backtesting

&nbsp;&nbsp;&nbsp;&nbsp;My rolling backtest went through each weight class with each classifier parameter and chose which parameters gave us the biggest return. Every weight class saw a positive return against the sportsbooks. I also introduced a threshold to each weight class, which changes depending on how much the results vary for that weight class. A higher threshold means that the expected value must be higher for the model to tell you to bet. A weight class with a lower threshold will tell you to take more risk and bet on fighters with a lower expected value. The model's prediction will tell you who it expects to win, the confidence level, and who to bet on. The model may tell you to bet on a fighter who is expected to lose because given the expected chance of victory, they should make you money.

## 4. The Front End

&nbsp;&nbsp;&nbsp;&nbsp;I built the front end using react and tailwind. My front end experience is limited, so I stuck with what I know and built something basic. I was able to connect the front end directly to my PostgreSQL database in RDS, so once data is pushed live to the cloud, my front end will update. I have my front end stored in an S3 bucket, so CodeBuild and CodePipeline will automatically update the live site once any changes are pushed to my main GitHub branch.

## 5. The Back End

&nbsp;&nbsp;&nbsp;&nbsp;Everything in the back end is held together and ran using FastAPI. This was my first experience using FastAPI and, like python, it is very readable and easy to pick up. It worked perfectly for everything I needed in this project

### 5a. The Data

&nbsp;&nbsp;&nbsp;&nbsp;We have a few different datasets in here used for a few different things. In our database folder, we have the schema for all_fights and historical_predictions. All_fights is our main dataset to backtest and train our models on. It will have all fight data in it, including results. Historical_predictions will have all of the pre-fight data but no winner, only the prediction made by the model. We can look back at historical_predictions to see how our model performed with fights in the past.

&nbsp;&nbsp;&nbsp;&nbsp;We have some generated data when we run the scrapers: upcoming_fights, upcoming_fights_metadata, recent_fights, and some missing data reports to look at. Recent_fights is added to all_fights when generated and historical_predictions is a combination of upcoming_fights and upcoming_fights_metadata. 

### 5b. The Scrapers

&nbsp;&nbsp;&nbsp;&nbsp;My historical fight data scraper looks back at all previous fights between a start data and present day, takes in all of the fight information and adds it to a CSV. We cannot see fighters' statistics, like their pre-fight average strike numbers, so we use their average as of the day of the scrape for any fight in the time frame. We get our fight odds from fightodds.io and our historical ranking information from martj42's kaggle dataset. 

&nbsp;&nbsp;&nbsp;&nbsp;Upcoming scraper looks at the next UFC event from the UFC stats website and takes in all of the fighters' current information, the fight data, and the odds from the fight odds website. Right now we still use martj42's kaggle dataset to keep the same rankings scraper as the historical scraper, but I plan to update that in the future. 

### 5c. Model Training and API Integration

&nbsp;&nbsp;&nbsp;&nbsp;For the model training section, I just copied my code from my backtesting notebook. Code that wasn't previously a function was made into one. My FastAPI app is used to run the scrapers, generate predictions, and load csv files into the database. There is a separate README.md for the back end that explains how to use everything.

## 6. Looking ahead

&nbsp;&nbsp;&nbsp;&nbsp;There are a couple of things I'd like to do to improve this project. Firstly, I'd love to automate it so that it updates the predictions every few days before the fights and automatically finishes the fights once the event is complete. This way I could just let it run and not worry about it, only checking in every week to make sure that the proper data is added to the databse and no errors occurred. 

&nbsp;&nbsp;&nbsp;&nbsp;I'd also like to expand the UI to include some statistics or an interactive interface so the user can see how my models tend to perform. A wider front end would give the user more confidence in what I've built.