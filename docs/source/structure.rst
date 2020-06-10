Project structure
*****************

The following aims to explain the project structure

..
   Note: Built with ``tree -d``, pre-cleaned with the dangerous
   ``find . | grep -E "(__pycache__|\.pyc|\.pyo$)" | xargs rm -rf``

.. code::

   .
   ├── bout_runners            #
   │   ├── database            #
   │   ├── executor            #
   │   ├── log                 #
   │   ├── make                #
   │   ├── metadata            #
   │   ├── parameters          #
   │   ├── runner              #
   │   ├── submitter           #
   │   └── utils               #
   ├── config                  #
   ├── docker                  #
   ├── docs                    #
   │   └── source              #
   │       ├── _static         #
   │       ├── _templates      #
   │       ├── api             #
   │       └── examples        #
   ├── logs                    #
   └── tests                   #
       ├── data                #
       ├── integration         #
       │   └── bout_runners    #
       │       └── runners     #
       └── unit                #
           └── bout_runners    #
               ├── database    #
               ├── executor    #
               ├── log         #
               ├── make        #
               ├── metadata    #
               ├── parameters  #
               ├── submitter   #
               └── utils       #
