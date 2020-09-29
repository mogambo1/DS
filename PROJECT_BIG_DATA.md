#### Hemant Taneja - Project



#### Introduction and problem description

Amazon review database includes partition on product categories which then include different customer ids,  product id, review id and several others. The basis of analysis needed us to examine the dataset and find out particular verticals  on which the customer behavior can be explained which in turn can help and recommend better products for a better sales. Customer ratings on a particular product in a year gives an indication of what others might think about the same product. Some products have lower rating than other products for which we can identify the dynamics as to at what point in time and which customer id has shown his review rating to be unsatisfactory.

#### Data Cleaning

This step took major chunk of thought as the dataset had duplicate values which we had to get rid of. So I created o filtered dataset in which multiple reviews by the same users for the same product was filtered. After which the analysis required us to show the results for year >= 2005 also only for the product categories which are like Wireless , Automotive ,Music ,Digital_Music_Purchase ,Sports ,Toys, Digital_Video_Games  and Video_Games

##### Create Table with Excluded data:

```sql
CREATE TABLE amazon_review.amazon_review_filtered_data
AS
SELECT a.* from
(SELECT *,
row_number()
OVER (partition by customer_id, product_id) AS row_num
FROM (SELECT *
FROM amazon_review.amazon_reviews_parquet
WHERE review_id IN
(SELECT x.review_id
FROM
(SELECT customer_id, product_id, review_id, count(*)
FROM amazon_review.amazon_reviews_parquet
GROUP BY customer_id, product_id,review_id
HAVING (count(*)) =1)as x)and product_category IN
('Wireless','Automotive','Music','Digital_Music_Purchase','Sports','Toys','Digital_Video_Games','Video_Games'))as y)as a
WHERE row_num=1;
```

#### Basic Exploratory Analysis:

1. Explore the dataset and provide basic exploratory analysis: 

   1. Number of reviews 

   2. Number of users 

   3. Average review stars 

   4. Average length of the review 

   5. Number of verified versus unverified reviews 

   6. At least two more additional metrics 

   7. Provide trending (over time) analysis of each of the metrics above


```sql
SELECT year,
count(review_id) as Review_count,
count(distinct(customer_id)) as User_count,
round(avg(star_rating),2) as Average_Rating,
round(avg(length(review_body)),2) as Avg_Length_of_Review,
sum(case
WHEN verified_purchase ='Y' THEN 1
ELSE 0 end) as Verified_Users, 
sum(case
WHEN verified_purchase='N' THEN 1
ELSE 0 end) as Unverified_Users, 
sum(case
WHEN helpful_votes= 1 THEN 1
ELSE 0 end) as Total_Helpful_Votes, 
count(distinct(product_id)) as Total_Products
FROM amazon_review.amazon_review_filtered_data
WHERE year>=2005
GROUP BY year
ORDER BY year;
```

**Visualization:**

Here, we can see that number of customers are increasing gradually from 2008 till 2014 but after 2014, there is no significant growth seen.

##### Detailed analysis of Music/Digital_Music_Purchase and Digital_Video_Games/Video_Games over time.

**Music and Digital_Music_Purchase**

Correlation between the categories over time

```sql
SELECT year,
sum(case
WHEN product_category ='Music'THEN 1
ELSE 0 end) AS music_customers, 
sum(case
WHEN product_category ='Digital_Music_Purchase' THEN 1
ELSE 0 end) AS digital_music_customers
FROM amazon_review.amazon_review_filtered_data
WHERE year>=2005
GROUP BY year
ORDER BY year;
```

**Output:**

There was no drastic change seen in the number of customers from 2005 to 2012 for Music category but
after 2012 to 2014 number of customer got increased, also a drop wass seen after 2014 in the customer count. And, for Digital Music, initially on 2005 there was not a single customer who reviewed digital music products but from 2007 number of customers started increasing till 2014.

**Video_Games and Digital_Video_Games.**

```sql
SELECT year,
sum(case
WHEN product_category ='Video_Games' THEN 1
ELSE 0 end) AS video_game_customers, 
sum(case
WHEN product_category ='Digital_Video_Games' THEN 1
ELSE 0 end) AS digital_video_game_customers
FROM amazon_review.amazon_review_filtered_data
WHERE year>=2005
GROUP BY year
ORDER BY year;
```

**Output:**

For Video_game ,we can see that the number of customers gets increased from 2005 to 2012.Also,  after
2012 to 2014 number of customer increased drastically. Similarly, for Digital video games,
initially from 2005 to 2008 there was not a single customer reviewing digital music products but
after 2008 up until 2014 number of customers got increased. After 2014, number of customers
started decreasing again.

### Are there same users reviewing in both categories?

To find out whether there are same customer who are reviewing for products in both categories:

**Music and Digital_Music_Purchase**

```sql
SELECT count(a.customer_id) as count,a.year
FROM amazon_review.amazon_review_filtered_data as a,
(SELECT distinct(customer_id)
FROM amazon_review.amazon_review_filtered_data
WHERE product_category='Music'
AND year>=2005 intersect SELECT distinct(customer_id)
FROM amazon_review.amazon_review_filtered_data
WHERE product_category='Digital_Music_Purchase' AND year>=2005) as z
WHERE a.customer_id=z.customer_id AND a.year>=2005 AND a.product_category 
IN ('Music','Digital_Music_Purchase')
GROUP BY year
ORDER BY year;
```

**Output:**

Here, we can see that, in 2014 number of customers reviewing both Music and Digital music categories were more as compared to any other year.

**Video_Games and Digital_Video_Games.**

```sql
SELECT count(a.customer_id) AS count,a.year
FROM amazon_review.amazon_review_filtered_data as a,
(SELECT distinct(customer_id)
FROM amazon_review.amazon_review_filtered_data
WHERE product_category='Video_Games'
AND year>=2005 intersect SELECT distinct(customer_id)
FROM amazon_review.amazon_review_filtered_data
WHERE product_category='Digital_Video_Games' AND year>=2005) as z
WHERE a.customer_id=z.customer_id AND a.year>=2005 AND a.product_category 
IN ('Video_Games','Digital_Video_Games')
GROUP BY year
ORDER BY year;
```

**Output:**

Here, we can see that in 2014 number of customers reviewing both Video_Games and Digital video game categories were more as compared to any other year.

#### Total Number of same users reviewing in both categories

##### Music and Digital_Music_Purchase categories:

```sql
SELECT count(a.customer_id) AS Customer_count
FROM amazon_review.amazon_review_filtered_data as a,
(SELECT distinct(customer_id)
FROM amazon_review.amazon_review_filtered_data
WHERE product_category='Music'
AND year>=2005 intersect SELECT distinct(customer_id)
FROM amazon_review.amazon_review_filtered_data
WHERE product_category='Digital_Music_Purchase' AND year>=2005) as z
where a.customer_id=z.customer_id AND a.year>=2005 AND a.product_category 
IN ('Music','Digital_Music_Purchase');
```



**Video games and Digital_Video_Games categories:**

```sql
SELECT count(a.customer_id) AS Customer_count
FROM amazon_review.amazon_review_filtered_data as a,
(SELECT distinct(customer_id)
FROM amazon_review.amazon_review_filtered_data
WHERE product_category='Video_Games'
AND year>=2005 intersect SELECT distinct(customer_id)
FROM amazon_review.amazon_review_filtered_data
WHERE product_category='Digital_Video_Games' AND year>=2005) as z
where a.customer_id=z.customer_id AND a.year>=2005 AND a.product_category 
IN ('Video_Games','Digital_Video_Games');
```



##### Can you identify similar items in both categories? Do they get same rating?

##### 	Analysis between Music and Digital music category:

```sql
SELECT mc.product_id , Music_Rating, Digital_Music_Rating
FROM 
(SELECT product_id, round(avg(star_rating),2) AS Music_Rating
FROM amazon_review.amazon_review_filtered_data
WHERE product_category='Music'
AND year>= 2005
GROUP BY product_id) as mc
INNER JOIN 
(SELECT product_id, round(avg(star_rating),2) AS Digital_Music_Rating
FROM amazon_review.amazon_review_filtered_data
WHERE product_category='Digital_Music_Purchase'
AND year>= 2005
GROUP BY product_id) as dmc
ON mc.product_id=dmc.product_id;
```

Now, from the output we can say that we have same item in both Music and Digital_Music_Purchase
categories with different ratings.

##### 	Analysis between Video_Games and Digital_Game_Ranking category:

```sql
SELECT gc.product_id , Game_Rating, Digital_Game_Rating
FROM 
(SELECT product_id, round(avg(star_rating), 2) AS Game_Rating
FROM amazon_review.amazon_review_filtered_data
WHERE product_category='Video_Games'
AND year>= 2005
GROUP BY product_id) as gc
INNER JOIN 
(SELECT product_id, round(avg(star_rating), 2) AS Digital_Game_Rating
FROM amazon_review.amazon_review_filtered_data
WHERE product_category='Digital_Video_Games'
AND year>= 2005
GROUP BY product_id) as dgc
ON gc.product_id=dgc.product_id;
```



#### Hive Advanced Functions

##### 	Ranking based on products under different product categories

Calculated rank of different products in specific product category to find out popular products and least rated products in that category.

```sql
SELECT z.product_id, z.product_category, z.star_rank
FROM
(SELECT a.product_id, a.product_category,
Row_number()
OVER (partition by a.product_category
ORDER BY a.avg_rating desc) AS star_rank
FROM
(SELECT product_id, product_category, avg(star_rating) AS avg_rating
FROM amazon_review.amazon_review_filtered_data
WHERE year>= 2005
GROUP BY product_id,product_category)as a)as z
WHERE z.star_rank<=5;
```

**Output:**

Here, we can see products represented in light color have low rating as compared to products represented in darker shade. Hence we can identify the products which need more assistance for product improvement.

#### Compared the growth of the products by calculating moving average:

​	**Calculated moving average for product categories to evaluate growth.**

```sql
SELECT a.product_category,a.year,a.cnt as count,
(case
WHEN row_number()
OVER (partition by a.product_category order by a.year) > 4 THEN
round(AVG(a.cnt)
OVER (PARTITION BY a.product_category order by a.year ROWS 4 PRECEDING)) end) AS five_year_moving_avg
FROM
(SELECT product_category, year, count(review_id) AS cnt
FROM amazon_review.amazon_review_filtered_data
GROUP BY product_category,year
ORDER BY year desc) AS a
WHERE a.year >= 2005;
```

##### Maximum Rating of a product in specific product category:

Calculated maximum rating of a product in specific product category to find out most popular product in that category.

```sql
SELECT a.product_id, a.product_category, round(max(a.avg_rating),2) AS Max_Rating
FROM
(SELECT product_id, product_category, avg(star_rating) as avg_rating
FROM amazon_review.amazon_review_filtered_data
WHERE year>= 2005
GROUP BY product_id, product_category) as a
GROUP BY a.product_id, a.product_category;
```

**Output:**

Here, we can see that digital_music_Purchase, as a category, has the maximum average rating.

**Minimum Rating of a product in specific product category:**

```sql
SELECT a.product_id, a.product_category, round(min(a.avg_rating),2) AS Min_Rating
FROM
(SELECT product_id, product_category, avg(star_rating) as avg_rating
FROM amazon_review.amazon_review_filtered_data
WHERE year>= 2005
GROUP BY product_id, product_category) as a
GROUP BY a.product_id, a.product_category;
```

**Output:**

Here, we can see that digital_video_games, as a category, has the minimum average rating.

**Calculated Standard Deviation to analyze normal distribution of star rating of particular**
**product category**

```sql
SELECT year,product_category,round(stddev(star_rating),2)as Standard_Deviation,
round(avg(star_rating),2) as Avg_Rating
FROM amazon_review.amazon_review_filtered_data
WHERE year>= 2005
GROUP BY product_category, year;
```



**References:**

1) https://mode.com/sql-tutorial/sql-window-functions/ 

2) https://www.w3schools.com/sql/

3) https://stackoverflow.com/