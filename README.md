# Fight-Result-Predictor

## Project Layout

- `backend/`
  - Python scrapers, future backend API code, database schema files, and backend env files
- `frontend/`
  - Reserved for the future web application
- `data/`
  - Existing project CSVs used for modeling and notebook work
- `models/`
  - Saved model artifacts
- `notebooks/`
  - Exploration, preprocessing, and modeling notebooks

## 1. Introduction
&nbsp;&nbsp;&nbsp;&nbsp;For my final project as a WGU student, I wanted to build something to really tie together a lot of the concepts I've learned during my time here. There are many concepts that we touched upon at WGU which I'd like to expand on. I want this project to tie together front-end and back-end programming, machine learning, and a CI/CD pipeline.

### 1a. My Idea
&nbsp;&nbsp;&nbsp;&nbsp;I wanted to build a machine learning model with measurable results and real-world effects that could be continuously trained and used. Sports betting is not something I'm particularly interested in, but sports-watching is. Combat sports are my favorite, but they can see highly varying results. While odds-makers can often predict the fight very well, age, style, momentum, and history can also play into things. I want to build a model that can take into account all aspects of a fight and how each fighter has faired previously in similar circumstances. 

### 1b. My Plan
&nbsp;&nbsp;&nbsp;&nbsp;My model will predict the results of a fight and return a level of confidence, determining how much the bettor should place on the fight and which fighter they should bet on. This confidence level will account for betting lines and upset likelihood, so fights that appear to be more one-sided will see higher bets. My goal is for the model to be profitable.

## 2. Data Entry and Cleaning
&nbsp;&nbsp;&nbsp;&nbsp;During this step, I had to manually enter some data after finding out that the dataset I used didn't properly scrape the data for every fight. Since there were around 600 rows with missing data, I decided to add in what was missing in chunks and discard the remaining 150 or so.

&nbsp;&nbsp;&nbsp;&nbsp;Next I had to play with some datatypes. I wanted object columns that only said Red or Blue to be changed to boolean to help the computer, and I wanted to make as many float columns into integers as I could. The date column didn't come as a datetime object so I had to change that over as well. I knocked out all old and irrelevant data before October of 2018, so this dataset will start with bouts on or after UFC 229.

&nbsp;&nbsp;&nbsp;&nbsp;Down at the bottom, I did some validation testing with values I thought might be a little bit tricky for the data scraper. The data scraper passed the test with the values it did bring to us, so that means we may not need to change to much when we run it ourselves. Once everything looked good, I saved the 3081 fights as a csv to be used for modeling.
