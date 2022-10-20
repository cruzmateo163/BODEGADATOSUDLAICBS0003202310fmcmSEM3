
USE fmcmdbsor;
CREATE TABLE CHANNELS 
    ( 
     CHANNEL_ID INTEGER  NOT NULL , 
     CHANNEL_DESC VARCHAR (20)  NOT NULL , 
     CHANNEL_CLASS VARCHAR (20)  NOT NULL , 
     CHANNEL_CLASS_ID INTEGER NOT NULL 
    );

ALTER TABLE CHANNELS 
    ADD CONSTRAINT CHANNELS_PK PRIMARY KEY ( CHANNEL_ID );


CREATE TABLE COUNTRIES 
    ( 
     COUNTRY_ID INTEGER  NOT NULL , 
     COUNTRY_NAME VARCHAR (40) NOT NULL , 
     COUNTRY_REGION VARCHAR (20)  NOT NULL , 
     COUNTRY_REGION_ID INTEGER  NOT NULL 
    );

ALTER TABLE COUNTRIES 
    ADD CONSTRAINT COUNTRIES_PK PRIMARY KEY ( COUNTRY_ID ) ;


CREATE TABLE CUSTOMERS 
    ( 
     CUST_ID INTEGER  NOT NULL , 
     CUST_FIRST_NAME VARCHAR (20 )  NOT NULL , 
     CUST_LAST_NAME VARCHAR (40 )  NOT NULL , 
     CUST_GENDER CHAR (1 )  NOT NULL , 
     CUST_YEAR_OF_BIRTH INTEGER (4)  NOT NULL , 
     CUST_MARITAL_STATUS VARCHAR (20 ) , 
     CUST_STREET_ADDRESS VARCHAR (40 )  NOT NULL , 
     CUST_POSTAL_CODE VARCHAR (10 )  NOT NULL , 
     CUST_CITY VARCHAR (30 )  NOT NULL , 
     CUST_STATE_PROVINCE VARCHAR (40)  NOT NULL , 
     COUNTRY_ID INTEGER  NOT NULL , 
     CUST_MAIN_PHONE_NUMBER VARCHAR (25)  NOT NULL , 
     CUST_INCOME_LEVEL VARCHAR (30 ) , 
     CUST_CREDIT_LIMIT INTEGER , 
     CUST_EMAIL VARCHAR (30 )
    );

ALTER TABLE CUSTOMERS 
    ADD CONSTRAINT CUSTOMERS_PK PRIMARY KEY ( CUST_ID )  ;


CREATE TABLE PRODUCTS 
    ( 
     PROD_ID INTEGER (6)  NOT NULL , 
     PROD_NAME VARCHAR (50 )  NOT NULL , 
     PROD_DESC VARCHAR (4000 )  NOT NULL , 
     PROD_CATEGORY VARCHAR (50 )  NOT NULL , 
     PROD_CATEGORY_ID INTEGER  NOT NULL , 
     PROD_CATEGORY_DESC VARCHAR (2000 )  NOT NULL , 
     PROD_WEIGHT_CLASS  INTEGER (3)  NOT NULL , 
     SUPPLIER_ID  INTEGER (6)  NOT NULL , 
     PROD_STATUS VARCHAR (20 )  NOT NULL , 
     PROD_LIST_PRICE DECIMAL (8,2)  NOT NULL , 
     PROD_MIN_PRICE DECIMAL (8,2)  NOT NULL 
    );

ALTER TABLE PRODUCTS 
    ADD CONSTRAINT PRODUCTS_PK PRIMARY KEY ( PROD_ID ) ;


CREATE TABLE PROMOTIONS 
    ( 
     PROMO_ID  INTEGER (6)  NOT NULL , 
     PROMO_NAME VARCHAR (30 )  NOT NULL , 
     PROMO_COST DECIMAL (10,2)  NOT NULL , 
     PROMO_BEGIN_DATE DATE  NOT NULL , 
     PROMO_END_DATE DATE  NOT NULL 
    );

ALTER TABLE PROMOTIONS 
    ADD CONSTRAINT PROMO_PK PRIMARY KEY ( PROMO_ID )  ;


CREATE TABLE SALES 
    ( 
     PROD_ID INTEGER (6)  NOT NULL , 
     CUST_ID  INTEGER  NOT NULL , 
     TIME_ID DATE  NOT NULL , 
     CHANNEL_ID INTEGER  NOT NULL , 
     PROMO_ID INTEGER (6)  NOT NULL , 
     QUANTITY_SOLD DECIMAL (10,2)  NOT NULL , 
     AMOUNT_SOLD DECIMAL (10,2)  NOT NULL 
    ) ;



CREATE TABLE TIMES 
    ( 
     TIME_ID DATE  NOT NULL , 
     DAY_NAME VARCHAR (9 )  NOT NULL , 
     DAY_NUMBER_IN_WEEK INTEGER (1)  NOT NULL , 
     DAY_NUMBER_IN_MONTH INTEGER (2)  NOT NULL , 
     CALENDAR_WEEK_NUMBER INTEGER (2)  NOT NULL , 
     CALENDAR_MONTH_NUMBER INTEGER (2)  NOT NULL , 
     CALENDAR_MONTH_DESC VARCHAR (8 )  NOT NULL , 
     END_OF_CAL_MONTH DATE  NOT NULL , 
     CALENDAR_MONTH_NAME VARCHAR (9 )  NOT NULL , 
     CALENDAR_QUARTER_DESC CHAR (7)  NOT NULL , 
     CALENDAR_YEAR INTEGER (4)  NOT NULL 
    ) ;


ALTER TABLE TIMES 
    ADD CONSTRAINT TIMES_PK PRIMARY KEY ( TIME_ID ) ;


ALTER TABLE CUSTOMERS 
    ADD CONSTRAINT CUSTOMERS_COUNTRY_FK FOREIGN KEY 
    ( 
     COUNTRY_ID
    ) 
    REFERENCES COUNTRIES 
    ( 
     COUNTRY_ID
    ) ;


ALTER TABLE SALES 
    ADD CONSTRAINT SALES_CHANNEL_FK FOREIGN KEY 
    ( 
     CHANNEL_ID
    ) 
    REFERENCES CHANNELS 
    ( 
     CHANNEL_ID
    ) ;


ALTER TABLE SALES 
    ADD CONSTRAINT SALES_CUSTOMER_FK FOREIGN KEY 
    ( 
     CUST_ID
    ) 
    REFERENCES CUSTOMERS 
    ( 
     CUST_ID
    ) 
;


ALTER TABLE SALES 
    ADD CONSTRAINT SALES_PRODUCT_FK FOREIGN KEY 
    ( 
     PROD_ID
    ) 
    REFERENCES PRODUCTS 
    ( 
     PROD_ID
    ) ;


ALTER TABLE SALES 
    ADD CONSTRAINT SALES_PROMO_FK FOREIGN KEY 
    ( 
     PROMO_ID
    ) 
    REFERENCES PROMOTIONS 
    ( 
     PROMO_ID
    ) ;


ALTER TABLE SALES 
    ADD CONSTRAINT SALES_TIME_FK FOREIGN KEY 
    ( 
     TIME_ID
    ) 
    REFERENCES TIMES 
    ( 
     TIME_ID
    ) 
;
