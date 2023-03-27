# Data Engineering HW Redone
My improved solution to the Vinted Engineering Academy Data Engineering homework   

This is a python project, python must be installed with a compatible IDE.
Developed and tested using Visual Studio Code  

>Download the files into an empty folder. Open the folder in an IDE (such as Visual Studio).   
In the terminal, run with:  
python Task1.py   (For the first task)  
python Task2.py   (For the second task)   
The program will print out to the console which allows for a quick glance at the output format  
Running these commands will call the functions with the necessary inputs to complete tasks 1 and 2   

The MapReduce framework is located, fittingly, in the framework.py file.  
As the title suggests, this is not my first solution to the homework.  
This solution makes use of higher order functions to hopefully achieve more simplicity and more scalability.
What I would do next is focus on the following limitations in the framework. The framework does not properly have the ability to intake a dictionary of map functions and process them. The merge system is done in a way that requires calling the function twice. The key is taken as the first output column entry, rather than defined in the user map function. The merge could probably be done at the map level and then sent to the reduce function. These are priority areas I would try to improve.
