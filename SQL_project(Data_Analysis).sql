
                                -- PROJECT OF MYSQL FOR DATA CLEANING AND EXPLORITARY DATA ANALYSIS --

                                              -- STAGE 1 : DATA CLEANING --

-- begin by bringing the data in schemas --

SELECT * FROM layoffs;

-- creating a working table to let the orginal data be safe --

create table layoffs_stage1
like layoffs ;

insert layoffs_stage1
select * from layoffs ;

select * from layoffs_stage1 ;

-- editing assigning row_numbers for duplicate columns --

select *,
row_number() over(partition by 
location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions ) as row_num
from layoffs_stage1 ;

-- creating a CTE table for identifying duplicate values --

with duplicate_cte as
(select *,
row_number() over(partition by 
location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions ) as row_num
from layoffs_stage1 
)
select * from duplicate_cte
where row_num > 1 
;


-- to delete we have to create another staging table and delete from the new table since we cannot update the cte in mysql --

CREATE TABLE `layoffs_stage2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
   `row_num`  int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


insert into layoffs_stage2                             -- inserting into new table --
select *,
row_number() over(partition by 
location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions ) as row_num
from layoffs_stage1 ;

select * from layoffs_stage2
where row_num > 1 ;
                                                 --  now deleting the duplicates --
 
 SET SQL_SAFE_UPDATES = 0;  -- this only disables safe mode for only this session --

 delete from layoffs_stage2
where row_num > 1;

select * from  layoffs_stage2 ;


 -- standardizing data---
                                                 
select company , trim(company)              -- using trim --
 from layoffs_stage2 ;

update layoffs_stage2 
set company = trim(company);


select distinct
industry                              
from layoffs_stage2 ;                            -- grouping the similar data -- 

update layoffs_stage2 
set industry = 'crypto'
where  industry like '%crypto%' ;


select distinct
location                                             -- identify any abnormalty -- 
 from layoffs_stage2 ;
 
 -- dusseldorf florianapoliss  Malmö           'DÃ¼sseldorf'  'FlorianÃ³polis' MalmÃ¶                these are junk words--
update layoffs_stage2
SET location = CASE
    WHEN location LIKE '%sseld%' THEN 'dusseldorf'
    WHEN location LIKE '%Florian%' THEN 'florianapoliss'
    WHEN location LIKE '%Malm%' THEN 'Malmö'                           -- updated with relevent name of places -- 
    ELSE location  -- Keeps other values unchanged
END;


-- deleting the Null valus and unesseray columns --

select * from layoffs_stage2
where total_laid_off is null 
and
percentage_laid_off is null 
; 

delete from layoffs_stage2
where total_laid_off is null 
and
percentage_laid_off is null 
; 
 
alter table layoffs_stage2
drop column row_num ;

SELECT DATE_, STR_TO_DATE(DATE_, '%m/%d/%Y') AS converted_date
FROM layoffs_stage2
WHERE DATE_ IS NOT NULL AND DATE_ != '';

alter table layoffs_stage2
change date DATE_  TEXT ;

UPDATE layoffs_stage2
SET DATE_ = STR_TO_DATE(DATE_, '%m/%d/%Y')
WHERE STR_TO_DATE(TRIM(DATE_), '%m/%d/%Y') IS NOT NULL;





                                     --  STAGE 2 : DATA EXPLORATION AND DATA ANALYSIS --
                                     
    -- rolling sum of total laidoff by months of every year --
    
    select 
	substring(DATE_,1,7) as date,
    sum(total_laid_off)
    from layoffs_stage2 
    where total_laid_off is not null and 'date' is not null
     group by date
     order by 1 asc;
    
   
with rolling_total as 
(
  select 
	substring(DATE_,1,7) as date_,
    sum(total_laid_off) as monthly_sum
    from layoffs_stage2 
    where total_laid_off is not null
     group by substring(DATE_,1,7)
     order by 1 asc
     ) 
     select *,
     sum(monthly_sum) over ( order by date_ )
     from rolling_total ;
     

-- layoffs ranking of comapanys in every year --

select company, year(DATE_),sum(total_laid_off)
from layoffs_stage2
WHERE total_laid_off is not null                                              
group by company , year(DATE_)
order by 2 asc ;

with company_year(company, years, total_laid_off) as 
(
select company, year(DATE_),sum(total_laid_off)
from layoffs_stage2
WHERE total_laid_off is not null                                              
group by company , year(DATE_)
																					-- this table shows which are the top 5 companies whic laidoffed the most in each year --
),
 company_rank as
(
select *,
dense_rank() over(partition by years order by total_laid_off desc ) as ranking
 from company_year 
 
 
)
select * from company_rank
where ranking <=5
;

