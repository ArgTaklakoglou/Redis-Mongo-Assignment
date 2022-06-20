######################################################################
#------------------------------REDIS---------------------------------#
######################################################################

library(redux)

#r <- redux::hiredis(
#  redux::redis_config(
#    host = "redis-13424.c78.eu-west-1-2.ec2.cloud.redislabs.com", 
#    port = "13424", 
#    password = "argyristakl_mcsba"))

r <- redux::hiredis(
  redux::redis_config(
    host = "127.0.0.1", 
    port = "6379"))


emails_sent <- read.csv(file = 'C:\\Users\\argir\\Υπολογιστής\\Redis\\RECORDED_ACTIONS\\emails_sent.csv', sep = ";")
modified_listings <- read.csv(file = 'C:\\Users\\argir\\Υπολογιστής\\Redis\\RECORDED_ACTIONS\\modified_listings.csv', sep = ";")
View(emails_sent)
View(modified_listings)

unique(emails_sent$UserID)

#Task1.1

january_modified_listings <- modified_listings[modified_listings$MonthID == 1 ,] #subset modified_listings in order to contain only information referring to January
View(january_modified_listings)
rownames(january_modified_listings) <- rownames(1:nrow(january_modified_listings))
nrow(january_modified_listings)

for (client in 1:nrow(january_modified_listings)) {
  if (january_modified_listings$ModifiedListing[client] == "1") {
    r$SETBIT("ModificationsJanuary", client ,"1")
  }
  else{
    r$SETBIT("ModificationsJanuary", client ,"0")
  }
}

r$BITCOUNT("ModificationsJanuary") # 9969 users modified their listing on January.

#Task1.2

r$BITOP("NOT","results","ModificationsJanuary")
r$BITCOUNT("results") #10031 users didn't modified their listing on January.
#9969 + 10031 = 20000 != 19999

#Task1.3

january_emails_sent <- emails_sent[emails_sent$MonthID == 1 ,] #subset emails_sent only for the month January.
View(january_emails_sent)
january_table <- table(january_emails_sent$UserID)
View(january_table)

february_emails_sent <- emails_sent[emails_sent$MonthID == 2 ,] #subset emails_sent only for the month February.
View(february_emails_sent)
february_table <- table(february_emails_sent$UserID) #Create a table to see how many emails each user received in February.
View(february_table)

march_emails_sent <- emails_sent[emails_sent$MonthID == 3 ,] #subset emails_sent only for the month March.
View(march_emails_sent)
march_table <- table(march_emails_sent$UserID) #Create a table to see how many emails each user received in March.
View(march_table)

first_merge <- merge(x=january_table, y=february_table, by="Var1", all=TRUE) #because it's impossible to merge 3 tables at once, I merge the frequency table for January and February. (NA means 0)
View(first_merge)

total_merge <- merge(x=first_merge, y=march_table, by.x="Var1", by.y="Var1", all.x=TRUE, all.y=TRUE) #I merge the frequency table for January and February with frequency table for March.
View(total_merge)


colnames(total_merge) <- c("userID", "January", "February", "March") #rename the columns in the frequency table total_merge. The first column refers to the UserId, the second refers to how many times the user recieved an email in January,...
total_merge[is.na(total_merge)] <- 0 #replace na's with 0


for (client in 1:nrow(total_merge)) {     # if January[client] > 0 means that this client has receive email in January.
  if (total_merge$January[client] > 0) {
    r$SETBIT("EmailsJanuary", client ,"1")
  }
}

for (client in 1:nrow(total_merge)) {     # if January[client] > 0 means that this client has receive email in February.
  if (total_merge$February[client] > 0) {
    r$SETBIT("EmailsFebruary", client ,"1")
  }
}

for (client in 1:nrow(total_merge)) {     # if January[client] > 0 means that this client has receive email in March.
  if (total_merge$March[client] > 0) {
    r$SETBIT("EmailsMarch", client ,"1")
  }
}


r$BITCOUNT("EmailsJanuary") #count the users who received email on January (count the 1 in the Bitmap-Setbit)
r$BITCOUNT("EmailsFebruary") #count the users who received email on January (count the 1 in the Bitmap-Setbit)
r$BITCOUNT("EmailsMarch")  #count the users who received email on January (count the 1 in the Bitmap-Setbit)
r$BITOP("AND","every_month_email_recieved",c("EmailsJanuary","EmailsFebruary" ,"EmailsMarch")) #Count the 1 in the bitmap. Count the userID's which received email in January, in February AND in March.
r$BITCOUNT("every_month_email_recieved")  #2668 is the result of the users which received email in January, in February AND in March.

#Task1.4
r$BITOP("AND","Jan_Mar_email_recieved",c("EmailsJanuary","EmailsMarch")) #Contain 1 for the userID's which received email in January AND in March.(not in February).
r$BITCOUNT("Jan_Mar_email_recieved") #count the 1 in the bitmap. Count the users which received email in January AND in March.(not in February).

r$BITOP("NOT", "Inversion_of_February", "EmailsFebruary") #in bitmap EmailsFebruary, it changes the ones to zeros (1 -> 0)
r$BITCOUNT("Inversion_of_February")  #count the 1 I get (0 in EmailsFebruary, which now have became 1)

r$BITOP("AND","Jan_Mar_but_not_Feb",c("Jan_Mar_email_recieved","Inversion_of_February")) 
r$BITCOUNT("Jan_Mar_but_not_Feb") #2417 is the result of the users which received email in January and in March but not in February.


#Task 1.5
#For January
jan_table <- as.data.frame(table(january_emails_sent$UserID, january_emails_sent$EmailOpened)) #create a table contains the usersID(Var1) in the first column. In the second column(Var2) contains 0 or 1(0 = he/she didn't open the email, 1=he/she opened the email) and Freq column that contains the count- total times that this happened.
View(jan_table)
#jan_table_dokimi_nea <- jan_table[]
#jan_table_final <- jan_table[!(duplicated(jan_table$Var1) & jan_table$Freq != 0), ] #remove the rows refers to userIDs which receive more than 1 email in January and they read 1 or more and they didn't read one or more of them.
#View(jan_table_final)
#colnames(jan_table) <- c("userID", "NotOpened_Opened", "CountTheTimes")

userID_Jan_not_open <- list() #I create an empty list, I want to fill this list with the userID that received at least one email in January and didn't open it/them.

for (i in 1:nrow(jan_table)) { #With this loop I want to fill the empty list.
  if ( (jan_table$Var2[i] == 0) & (jan_table$Freq[i] >= 1) ) { #I want Var2[i] == 0 because 0 means 'The user hasn't open the email' and Freq > 0 because I want to have happened at least 1 time.(to have receive at least 1 email in January and he/she hasn't opened it)
    userID_Jan_not_open <- append(userID_Jan_not_open, jan_table$Var1[i])
  }
}

userID_Jan_not_open #To see the ID's and check if I get what I have expected to get.

userID_Jan_not_open <- as.data.frame(userID_Jan_not_open)
View(userID_Jan_not_open)
View(january_modified_listings)

colnames(userID_Jan_not_open) <- c("UserID") # rename the column in order to to able to merge(do an inner join) userID_Jan_not_open and january_modified_listings 
recieve_notOpened_modification <- merge(x=userID_Jan_not_open, y=january_modified_listings, by="UserID", all=FALSE)
View(recieve_notOpened_modification)

for (i in 1:nrow(recieve_notOpened_modification)) {   
  if (recieve_notOpened_modification$ModifiedListing[i] == 1) {  #I want to have only those who did a modification, that's why recieve_notOpened_modification$ModifiedListing[i] == 1
    r$SETBIT("EmailsOpenedJanuary", i ,"1")
  }
}

r$BITCOUNT("EmailsOpenedJanuary") #count the userID's that received email on January, they didn't opened it but although they did a modification. (2807)

#Task1.6
#For February
february_modified_listings <- modified_listings[modified_listings$MonthID == 2 ,] #subset modified_listings in order to contain only information referring to February
View(february_modified_listings)
rownames(february_modified_listings) <- rownames(1:nrow(february_modified_listings))

View(february_emails_sent)
feb_table <- as.data.frame(table(february_emails_sent$UserID, february_emails_sent$EmailOpened)) #create a table contains the usersID in column Var1, 0 or 1 in Var2(0 = he/she didn't open the email, 1=he/she opened the email) and Freq column that contains the count(total times) that this happened.
View(feb_table)
#colnames(feb_table) <- c("userID", "NotOpened_Opened", "CountTheTimes") 

userID_Feb_not_open <- list()  #empty list. I will append in the loop, users' IDs' from those who have receive email on February and haven't opened it.

for (i in 1:nrow(feb_table)) {
  if ( (feb_table$Var2[i] == 0) & (feb_table$Freq[i] >= 1) ) { #I want Var2[i] == 0 because 0 means 'The user hasn't open the email' and Freq > 0 because I want to have happened at least 1 time.(to have receive at least 1 email in February and he/she hasn't opened it)
    userID_Feb_not_open <- append(userID_Feb_not_open, feb_table$Var1[i])
  }
}

userID_Feb_not_open #To see the ID's and check if I get what I have expected to get.

userID_Feb_not_open <- as.data.frame(userID_Feb_not_open)
View(userID_Feb_not_open)
View(february_modified_listings)

colnames(userID_Feb_not_open) <- c("UserID") # rename the column in order to to able to merge(do an inner join) userID_Feb_not_open and february_modified_listings 
recieve_notOpened_modification_Feb <- merge(x=userID_Feb_not_open, y=february_modified_listings, by="UserID", all=FALSE)
View(recieve_notOpened_modification_Feb)

for (i in 1:nrow(recieve_notOpened_modification_Feb)) {   
  if (recieve_notOpened_modification_Feb$ModifiedListing[i] == 1) {  #I want to have only those who did a modification, that's why recieve_notOpened_modification$ModifiedListing[i] == 1
    r$SETBIT("EmailsOpenedFebruary", i ,"1")
  }
}

r$BITCOUNT("EmailsOpenedFebruary") #count the userID's that received email on February, they didn't opened it. Although they did a modification.


#For March
march_modified_listings <- modified_listings[modified_listings$MonthID == 3 ,] #subset modified_listings in order to contain only information referring to March
View(march_modified_listings)
rownames(march_modified_listings) <- rownames(1:nrow(march_modified_listings))

View(march_emails_sent)
march_table <- as.data.frame(table(march_emails_sent$UserID, march_emails_sent$EmailOpened)) #create a table contains the usersID in column Var1, 0 or 1 in Var2(0 = he/she didn't open the email, 1=he/she opened the email) and Freq column that contains the count(total times) that this happened.
View(march_table)
#colnames(march_table) <- c("userID", "NotOpened_Opened", "CountTheTimes") 

userID_March_not_open <- list()  #empty list. I will append in the loop, users' IDs' from those who have receive email on March and haven't opened it.

for (i in 1:nrow(march_table)) {
  if ( (march_table$Var2[i] == 0) & (march_table$Freq[i] >= 1) ) { #I want Var2[i] == 0 because 0 means 'The user hasn't open the email' and Freq > 0 because I want to have happened at least 1 time.(to have receive at least 1 email in March and he/she hasn't opened it)
    userID_March_not_open <- append(userID_March_not_open, march_table$Var1[i])
  }
}

userID_March_not_open #To see the ID's and check if I get what I have expected to get.

userID_March_not_open <- as.data.frame(userID_March_not_open)
View(userID_March_not_open)
View(march_modified_listings)

colnames(userID_March_not_open) <- c("UserID") # rename the column in order to to able to merge(do an inner join) userID_March_not_open and march_modified_listings 
recieve_notOpened_modification_March <- merge(x=userID_March_not_open, y=march_modified_listings, by="UserID", all=FALSE)
View(recieve_notOpened_modification_March)

for (i in 1:nrow(recieve_notOpened_modification_March)) {   
  if (recieve_notOpened_modification_March$ModifiedListing[i] == 1) {  #I want to have only those who did a modification, that's why recieve_notOpened_modification$ModifiedListing[i] == 1
    r$SETBIT("EmailsOpenedMarch", i ,"1")
  }
}

r$BITCOUNT("EmailsOpenedMarch") #count the userID's that received email on March, they didn't opened it. Although they did a modification.


r$BITOP("OR","JanORFebORMar",c("EmailsOpenedJanuary","EmailsOpenedFebruary","EmailsOpenedMarch"))
r$BITCOUNT("JanORFebORMar")  #Count the numeber of userID's who recieved email on January OR on February Or on March, they didn't opened it but they did a modification.The result:4972


#Task1.7
View(january_modified_listings)
View(modified_listings)

#For January
View(jan_table)
#colnames(jan_table) <- c("userID", "NotOpened_Opened", "CountTheTimes") 

userID_Jan_open <- list()  #empty list. I will append in the list through the loop, users' IDs' from those who have received email on January and have opened it.

for (i in 1:nrow(jan_table)) {
  if ( (jan_table$Var2[i] == 1) & (jan_table$Freq[i] >= 1) ) { #I want Var2[i] == 1 because 1 means 'The user has opened the email' and Freq > 0 because I want to have happened at least 1 time.(to have receive at least 1 email in January and he/she hasn't opened it)
    userID_Jan_open <- append(userID_Jan_open, jan_table$Var1[i])
  }
}

userID_Jan_open <- as.data.frame(userID_Jan_open)
View(userID_Jan_open)
View(january_modified_listings)

colnames(userID_Jan_open) <- c("UserID") # rename the column in order to to able to merge(do an inner join) userID_Jan_open and january_modified_listings 
recieve_Opened_modification <- merge(x=userID_Jan_open, y=january_modified_listings, by="UserID", all=FALSE)
View(recieve_Opened_modification)

for (i in 1:nrow(recieve_Opened_modification)) {   
  if (recieve_Opened_modification$ModifiedListing[i] == 1) {  #I want to have only those who did a modification, that's why recieve_notOpened_modification$ModifiedListing[i] == 1
    r$SETBIT("EmailsOpenedJanuaryModifiedListing1", i ,"1")
  }
}
r$BITCOUNT("EmailsOpenedJanuaryModifiedListing1") #The total users who receive an email on January, they opened it and they did a modification.
r$BITCOUNT("EmailsOpenedJanuary") #The total users who receive an email on January, they DIDN'T opened it and they did a modification.

#2797 The total userID who receive an email on January, they opened it and they did a modification
#2807 The total userID who receive an email on January, they DIDN'T opened it and they did a modification.

#For February
userID_Feb_open <- list()  #empty list. I will append in the list through the loop, users' IDs' from those who have received email on February and have opened it.

for (i in 1:nrow(feb_table)) {
  if ( (feb_table$Var2[i] == 1) & (feb_table$Freq[i] >= 1) ) { #I want Var2[i] == 1 because 1 means 'The user has opened the email' and Freq > 0 because I want to have happened at least 1 time.(to have receive at least 1 email in February and he/she hasn't opened it)
    userID_Feb_open <- append(userID_Feb_open, feb_table$Var1[i])
  }
}

userID_Feb_open <- as.data.frame(userID_Feb_open)
View(userID_Feb_open)
View(february_modified_listings)

colnames(userID_Feb_open) <- c("UserID") # rename the column in order to to able to merge(do an inner join) userID_Feb_open and february_modified_listings 
recieve_Opened_modification <- merge(x=userID_Feb_open, y=february_modified_listings, by="UserID", all=FALSE)
View(recieve_Opened_modification)

for (i in 1:nrow(recieve_Opened_modification)) {   
  if (recieve_Opened_modification$ModifiedListing[i] == 1) {  #I want to have only those who did a modification, that's why recieve_notOpened_modification$ModifiedListing[i] == 1
    r$SETBIT("EmailsOpenedFebruaryModifiedListing1", i ,"1")
  }
}
r$BITCOUNT("EmailsOpenedFebruaryModifiedListing1")   #The total users  who receive an email on February, they opened it and they did a modification
r$BITCOUNT("EmailsOpenedFebruary")    #The total users who receive an email on February, they DIDN'T opened it and they did a modification.

#4297 The total userID who receive an email on February, they opened it and they did a modification
#2822 The total userID who receive an email on February, they DIDN'T opened it and they did a modification.

#For March

userID_Mar_open <- list()  #empty list. I will append in the list through the loop, users' IDs' from those who have received email on March and have opened it.

for (i in 1:nrow(march_table)) {
  if ( (march_table$Var2[i] == 1) & (march_table$Freq[i] >= 1) ) { #I want Var2[i] == 1 because 1 means 'The user has opened the email' and Freq > 0 because I want to have happened at least 1 time.(to have receive at least 1 email in March and he/she hasn't opened it)
    userID_Mar_open <- append(userID_Mar_open, march_table$Var1[i])
  }
}

userID_Mar_open <- as.data.frame(userID_Mar_open)
View(userID_Mar_open)
View(march_modified_listings)

colnames(userID_Mar_open) <- c("UserID") # rename the column in order to to able to merge(do an inner join) userID_Mar_open and march_modified_listings 
recieve_Opened_modification <- merge(x=userID_Mar_open, y=march_modified_listings, by="UserID", all=FALSE)
View(recieve_Opened_modification)

for (i in 1:nrow(recieve_Opened_modification)) {   
  if (recieve_Opened_modification$ModifiedListing[i] == 1) {  #I want to have only those who did a modification, that's why recieve_notOpened_modification$ModifiedListing[i] == 1
    r$SETBIT("EmailsOpenedMarchModifiedListing1", i ,"1")
  }
}
r$BITCOUNT("EmailsOpenedMarchModifiedListing1") #The total users  who receive an email on March, they opened it and they did a modification
r$BITCOUNT("EmailsOpenedMarch")        #The total users who receive an email on March, they DIDN'T opened it and they did a modification.

#2783 The total userID who receive an email on March, they opened it and they did a modification
#2818 The total userID who receive an email on March, they DIDN'T opened it and they did a modification.



r$BITOP("OR","JanORFebORMarOpenMod",c("EmailsOpenedJanuaryModifiedListing1","EmailsOpenedFebruaryModifiedListing1","EmailsOpenedMarchModifiedListing1"))
r$BITCOUNT("JanORFebORMarOpenMod") #Count the numeber of userID's who recieved email on January OR on February Or on March, they opened it but they did a modification.The result:4980 



######################################################################
#------------------------------MOGNO---------------------------------#
######################################################################


library(mongolite)
library(jsonlite)

#Task 2.1
m <- mongo(collection = "json_collection",  db = "Bikes_Assignment", url = "mongodb://localhost")
m$remove('{}')

json <- read.table("C:\\Users\\argir\\Downloads\\BIKES_DATASET\\BIKES\\files_list.txt", header = TRUE, sep="\n", stringsAsFactors = FALSE)

for (i in 1:nrow(json)) {
  x <- fromJSON(readLines(json[i,], warn=FALSE, encoding="UTF-8"))
  x$ad_data$Mileage<- as.numeric(gsub("[,km]", "", x$ad_data$Mileage))
  x$ad_data$`Make/Model`<-(gsub("'.*", "",  x$ad_data$`Make/Model`))   #delete all '10, '08,... in `Make/Model`.
  if(x$ad_data$Price == 'Askforprice') {
    x$ad_data$Price <- NULL
    x$ad_data$AskForPrice <- TRUE
  }
  else {
    # Convert price to a number
    x$ad_data$Price <- as.numeric(gsub("[€.]", "", x$ad_data$Price)) #delete the symbol € and the  . between the numbers.
    x$ad_data$AskForPrice <- FALSE

    if ((x$ad_data$Price < 250) | (x$ad_data$Price > 90000)){
      x$ad_data$Price <- NULL
      x$ad_data$AskForPrice <- TRUE
    }
    
    if(grepl( "Negotiable", x$metadata$model)== TRUE | grepl( "NEGOTIABLE", x$metadata$model) == TRUE | grepl( "negotiable", x$metadata$model) == TRUE ) {  #wherever you match the word Negotiable or the word NEGOTIABLE or the word negotiable in model field, put a True in Negotiable field.
      x$metadata$Negotiable = TRUE
    }
    
    else {
      x$metadata$Negotiable = FALSE
    }
    
  }
  
  x <- toJSON(x, auto_unbox = TRUE)
  m$insert(x)
}

#Task2.2
m$count()

#Task2.3
m$aggregate('
  [
    { 
      "$match":{
        "ad_data.Price": {
          "$exists" : true
        }
      }
    }, 
    {
      "$group":{
        "_id": null, "avarage_price":{"$avg": "$ad_data.Price"}, "count_ads_price_exist":{"$sum": 1}
          
        }
    }
  ]
')

#ad_data.Price":{"$exists" : true} means price not null

#Task2.4
m$aggregate('
  [
    {
      "$match":{
     
        "ad_data.Price":{
          "$exists": true
          }
        }
    },
    {
      "$group":{
         "_id": null, "maximum_price":{"$max": "$ad_data.Price"}, "minimum_price":{"$min": "$ad_data.Price"}
      }
    }
  ]
')


#Task2.5
m$aggregate('
  [
    {"$match": 
      {"metadata.Negotiable": true
      }
    },
    {"$group":
        {"_id": null, "count_negotiable_ads": {"$sum":1}
        }
    }
  ]
')

#Τask 2.7
m$aggregate('
     [
        {
          "$group": 
              {"_id": "$metadata.brand", 
              "Avarage_Price": {"$avg":"$ad_data.Price"}, 
              "count": {"$sum": 1}}},
        {
          "$sort": 
              {"Avarage_Price": -1}
      },
            {
               "$limit": 1
       }
  ]
')


#Task2.9
m$count('{"extras" : "ABS"}')

