# Analyzing Relationships among Social Vulnerability Index factors, Crime Cases and Covid Cases​

In our work, we have performed a set of analysis to understand relationship among three datasets. The reason behind choosing these three datasets is we assumed that there might be some correlations between social vulnerability factors and crime cases. Later we included covid affected cases and death cases to see if there is any effect of social vulnerability or crime cases on any pandemic situation.  

 

Our analysis follows to support our assumptions. Though correlation values are not that much high for each pair of attributes we have considered, there is a similarity of all the correlations for each state and this similarity is consistent across the state too.  

 

The first assertion is for each state all the correlations (SVI with Crime, Crime with Covid or SVI with Covid) behave same. If SVI with Crim correlation is higher for one state, the other two correlations follow the same. And if there is negative correlation (no correlation) for one state, that means that state does not have any dependencies among these three factors: Social vulnerability, Crime and Pandemic. 

 

We also assert this behavior of similarity of different correlation types is consistent across different state. All the states data we have performed experiments on validates the first assertion which results in our second assertion. In respect of social and overall governance aspect, we can assert that all the states act like a bubble where all the dependency relations stay there and does not affect other state much. 

 

According to the methodology of our experiments, we can also assert that the counties under one state defines the relationship among social, economic, good governance, capability of handling emergency situation like pandemic. As a result, we can come to another conclusion that localization characteristics of counties under the same state represents a clustering nature of different socio-economic aspects like social vulnerability, crime and pandemic.  

![Slide9](https://user-images.githubusercontent.com/17882265/187147458-b7650862-5bef-4606-8a5a-9d8873630055.jpg)


Assumption about datasets:  Each state may have similar kind of dependency relationship among SVI, crime and covid defined by counties ​

Insight 1:  Correlation values of   socio-economic status, access to transportation, household composition, minority status  with total crime cases  are  closer to each other per state.​

Insight 2:  Correlation values of SVI with covid and Crime with covid  are also closer to each other per state​

Assertion 1: From insight 2  we can say  SVI and crime has same kind of dependency relation on covid cases for each state​

Assertion 2: From insight 1 and 2, we can say assertion 1 is consistent across all the states we have considered. ​
