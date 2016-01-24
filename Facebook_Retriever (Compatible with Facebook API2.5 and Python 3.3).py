
# coding: utf-8

# In[1]:

# import packages
#!/usr/bin/env python
## This script is tested under Py33
"""
Revised based on scripts from Social-metrics.org

GO HERE FOR DEFINITIONS OF VARIABLES RETURNED BY API: 
https://developers.facebook.com/docs/reference/api/post/
     
"""
import sys
import urllib 
import urllib.request
import string
import simplejson
import sqlite3
import time
import datetime
from datetime import datetime, date, time
from pprint import pprint

import sqlalchemy
from sqlalchemy.orm import mapper, sessionmaker
from sqlalchemy import Column, Integer, String, ForeignKey, Text, Numeric, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import update
from types import *

import re


# In[ ]:

# AUTHENTICATE FACEBOOK API
clientid = '' #insert your actual Facebook app client id
clientsecret = '' 
fb_urlobj = urllib.request.urlopen('https://graph.facebook.com/oauth/access_token?grant_type=client_credentials&client_id=' + clientid + '&client_secret=' + clientsecret)
fb_token = fb_urlobj.read().decode(encoding="latin1")
print ("COPY AND PASTE THE TOKEN IN THE CODE BELOW:",fb_token)


# In[2]:

# DATA STORAGE IS MADE POSSIBLE THROUGH SQLite. The following declares SQLite database for the task

Base = declarative_base()
###########################
class STATUS(Base):
    __tablename__ = 'Posts'
    
    id = Column(Integer, primary_key=True)   #CHANGED from rowid. BE CAREFUL IF MIXING DBs (COULD BE 'ID')
    feed_id = Column(Integer)
    org_name = Column(String)
    FB_org_id = Column(String)
    link = Column(String)
    message_id = Column(String)
    content = Column(String)
    published_date = Column(String)   
    date_inserted = Column(DateTime)
    type = Column(String)
    picture_link = Column(String)
    link_name = Column(String)
    share_count = Column(Integer)
    message_output = Column(String)
    feed_organization = Column(String)

    def __init__(self, id, feed_id, org_name, FB_org_id, link, message_id, 
        content, published_date, date_inserted, type, picture_link, 
        link_name, share_count,message_output,feed_organization,
        ):
        
        self.feed_id = feed_id
        self.org_name = org_name
        self.FB_org_id = FB_org_id
        self.link = link
        self.message_id = message_id
        self.content = content 
        self.published_date = published_date
        self.date_inserted = date_inserted
        self.type = type
        self.picture_link = picture_link
        self.link_name = link_name
        self.share_count = share_count
        self.message_output = message_output
        self.feed_organization = feed_organization
       
    def __repr__(self):
       return "<Organization, Sender('%s', '%s')>" % (self.id, self.link)
       
class Pages(Base):
    __tablename__ = 'graph_urls' # this is the table for a list of Facebook pages you are interested in mining

    Org_ID = Column(Integer, primary_key=True)   
    Org_Name = Column(String)
    address = Column(String) #optional
    city = Column(String) #optional
    zip5 = Column(String) #optional
    State = Column(String) #optional
    Total_Revenue = Column(Integer) #optional
    Assets = Column(Integer) #optional               
    FB_url = Column(String) #optional  
    FB_ID = Column(String) #optional  
    rss_url = Column(String) #optional  
    profile_added_date = Column(DateTime) #optional  
    followers_count = Column(Integer) #optional  
    talking_about_count = Column(Integer) #optional  
    were_here_count = Column(Integer) #optional  
    checkins = Column(Integer) #optional      
    username = Column(String) #optional  
    name = Column(String) #optional  
    category = Column(String) #optional  
    company_overview = Column(String) #optional  
    website = Column(String) #optional  
    phone = Column(String) #optional  
    link = Column(String) #optional  
    location = Column(String) #optional  
    founded = Column(String) #optional  
    general_info = Column(String) #optional  
    mission = Column(String) #optional  
    about = Column(String) #optional  
    description = Column(String) #optional  
    parking = Column(String) #optional  
    is_community_page = Column(String) #optional  
    products = Column(String) #optional  
    awards = Column(String) #optional  
    public_transit = Column(String) #optional  

    def __init__(self, Org_ID, Org_Name, 
    address, city, zip5,  State, Total_Revenue, Assets,
    FB_url, FB_ID, rss_url, 
    profile_added_date, followers_count, talking_about_count, were_here_count, checkins,
    username, name, category, company_overview, website, phone, link, location, founded, 
    general_info, mission, about, description, parking, is_community_page, products, awards,
    public_transit,
        ):
        
        self.Org_Name = Org_Name
        self.address = address
        self.city = city
        self.zip5 = zip5
        self.State = State  
        self.Total_Revenue = Total_Revenue
        self.Assets = Assets        
        self.FB_url = FB_url
        self.FB_ID = FB_ID
        self.rss_url = rss_url
        self.profile_added_date = profile_added_date        
        self.followers_count = followers_count
        self.talking_about_count = talking_about_count
        self.were_here_count = were_here_count
        self.checkins = checkins
        self.username = username
        self.name = name
        self.category = category
        self.company_overview = company_overview
        self.website = website
        self.phone = phone
        self.link = link
        self.location = location
        self.founded = founded
        self.general_info = general_info
        self.mission = mission
        self.about = about
        self.description = description
        self.parking = parking
        self.is_community_page = is_community_page
        self.products = products
        self.awards = awards
        self.public_transit = public_transit

    def __repr__(self):
       return "<Organization, Sender('%s', '%s')>" % (self.rowid, self.page_url)


# In[ ]:

data_url_1 = 'https://graph.facebook.com/v2.5/'  # us 2.4 is also fine 
data_url_2 = '/feed?fields=from,message,picture,link,name,description,type,created_time,shares&limit=100&'
fb_token = ''

since = "since=2014-01-01"
until = "&until=2014-08-24&"

def get_data(kid):
    try:
        d = simplejson.loads(urllib.request.urlopen(data_url_1 + kid + data_url_2 + since + until + fb_token).read())         
    except Exception as e:
        print ("Error reading id %s, exception: %s" % (kid, e))
        return None
    print ("d.keys(): ", d.keys())   
    return d

def write_data(self, d, feed_id, organization):   

    feed_id = feed_id
    feed_organization = organization
    date_inserted = datetime.now()
    messages = d['data']
    print ("NUMBER OF MESSAGES IN THIS SET OF RESULTS:", len(messages))
    
    for message in d['data']:
        message_output = str(message)
        
        org_name = message['from']['name']
        FB_org_id = message['from']['id']
        
        if 'link' in message:
            link = message['link']      
        else:
            link = ''
        
        if 'name' in message:
            link_name = message['name']
        else:
            link_name = ''
            
        if 'shares' in message:       
            share_count = message['shares']['count']
        else:
            share_count = ''
        
        if 'message' in message:
            content = message['message']
            content = content.replace('\n','') 
        else:
            content = ''
            
        published_date = message['created_time']
        type = message['type']
        message_id = message['id']      
                      
        if 'picture' in message:
            picture_link = message['picture']  #If available, a link to the picture included w/ post
        else:
            picture_link = ''
        

        updates = self.session.query(STATUS).filter_by(published_date=published_date,
                content=content).all()
        if not updates:
            print ("inserting, to_user:", org_name)
            
            upd = STATUS(None, feed_id, org_name, FB_org_id, link, message_id, 
                content, published_date, date_inserted,
                type, picture_link, 
                link_name, share_count, message_output,
                feed_organization,
                )          
                
            self.session.add(upd)
        else:
            if len(updates) > 1:
                print ("Warning: more than one update matching to_user=%s, text=%s"                     % (to_user, content))
            else:
                print ("Not inserting, dupe..")


class Scrape:
    def __init__(self):    #maybe I have to put in "Base" instead of "self"? No!
        engine = sqlalchemy.create_engine("sqlite:///xxx.sqlite", echo=False)  # different DB name here
        Session = sessionmaker(bind=engine)
        self.session = Session()  #n.b. - IT'S NOT session = Session()
        Base.metadata.create_all(engine)

    def main(self):
    
        all_feeds = self.session.query(Pages).all()
        
        keys = []
        for feed in all_feeds: #[91:92]:
            feed_id = feed.Org_ID
            organization = feed.Org_Name
            kid = feed.FB_ID

            print ("\rprocessing id %s/%s  --  %s" % (feed_id, len(all_feeds), organization),
            sys.stdout.flush())

            d = get_data(kid)

            if not d:
                print ("THERE WAS NO 'D' RETURNED........MOVING TO NEXT ID")
                continue         
            if not 'data' in d:
                print ("THERE WAS NO 'D['DATA']' RETURNED........MOVING TO NEXT ID")
                continue
            if len(d['data'])==0:
                print ("THERE WERE NO STATUSES RETURNED........MOVING TO NEXT ID")
                continue
                
            write_data(self, d, feed_id, organization)   
            self.session.commit() 
            
            paging = d['paging']
            
            if 'next' in paging:
                next_page_url = paging['next']
                print ("HERE IS THE NEXT PAGE URL:", next_page_url)
            else:
                print ("THERE AIN'T NO NEXT PAGE FOR", feed_id)
                
            if next_page_url:
                print ("THERE WERE STATUSES ON THE FIRST PAGE! NOW MOVING TO GRAB EARLIER POSTS")
                count = 2
                while count < 100:
                    print ("------XXXXXX------ STARTING PAGE", count)
                    try:
                        d = simplejson.loads(urllib.request.urlopen(next_page_url).read())        
                    except Exception as e:
                        print ("Error reading id %s, exception: %s" % (kid, e))
                        break
                    print ("d.keys(): ", d.keys())  
                    if not d:
                        print ("THERE WERE NO STATUSES RETURNED.MOVING TO NEXT ID")    
                        break
                    if not 'data' in d:
                        print ("THERE WERE NO STATUSES RETURNED.MOVING TO NEXT ID")
                        print (d)
                        break
                    if len(d['data'])==0:
                        print ("THERE WERE NO STATUSES RETURNED.MOVING TO NEXT ID")
                        print (d)
                        break
                    write_data(self, d, feed_id, organization) 
                    self.session.commit()

                    print ("------XXXXXX------ FINISHED WITH PAGE", count)
                    
                    if 'paging' in d:
                        if 'next' in d['paging']:
                            next_page_url_temp = d['paging']['next']
                            next_page_url = next_page_url_temp
                            print ("AND HERE IS THE NEXT PAGE URL:", next_page_url)
                        else:
                            print ("THERE AIN'T NO NEXT PAGE FOR", feed_id)
                            print ("--------------> WE'VE REACHED THE LAST PAGE!!!! MOVING TO NEXT ID")
                            break
                   
                    count += 1
                    import time
                    time.sleep(2)
                    if count > 100:
                        print ("WE'RE AT PAGE 100!!!!!")
                        break
                
            else:
                print ("THERE AIN'T NO NEXT_PAGE_URL FOR FEED_ID", feed_id, " ---- GOING ON TO NEXT ID")
            
            self.session.commit()
            
        self.session.close()


if __name__ == "__main__":
    s = Scrape()
    s.main()

