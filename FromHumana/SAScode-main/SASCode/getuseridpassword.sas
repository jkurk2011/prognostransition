
/*Purpose: General macro for retrieving the encrypted user names and passwords for various systems that we connect to in SAS.
           Can be used to store personal ID's and associated passwords (again, encrypted), along with the standard logins like sb_sandbox.
           This is especially useful for when a password change is required, so that multiple spots in the code don't need to change, and
           we don't have the sb_sandbox getting locked, etc.
           Additional usernames and passwords can be added to the code through appropriately crafted %if %then statements like the ones
            already in there.
*/
%macro getUserIDPassword(
          System=Oracle, /*Alias for the system desired to be connected to.  Doesn't necessarily match formally with anything else. */
          Type=Standard, /*Default of standard will return the universal userID (assuming there is one). Otherwise, pass Personal, in which case
                           the global reference in ByRefUserID must contain the userID of the person calling it.*/
          ByRefUserID=a, /*Must be the NAME of a GLOBAL variable declared by the CALLING program.  This macro will put into it the applicable userID. */
          ByRefPassword=a) /*Must be the NAME of a GLOBAL variabel declared by the CALLING program.  This macro will put into it the applicable password.*/
          ;

%put &ByRefUserID;
%put &&ByRefUserID;
%put &&&ByRefUserID;

%if &Type = Personal and &&&ByRefUserID = jpn0217 %then %do;
  %let &&ByRefPassword = {sas002}6F5D9D5B133987A6538590AD0EEE3D69; 
%end;
%if &Type = Personal and &&&ByRefUserID = axg5942 %then %do;
  %let &&ByRefPassword = {SAS002}8083755C3ADD1E3233B998884E75016B;
%end;
%if &Type = Personal and &&&ByRefUserID = bpk3381 %then %do;
  %let &&ByRefPassword = {sas002}2FA39126301291A508B18F4A18DA5A8A; 
%end;
%if &Type = Personal and &&&ByRefUserID = pgk0233 %then %do;
  %let &&ByRefPassword = {SAS002}160E9F2733017EC22F06C84D05FF9BD2;
%end;
%if &Type = Personal and &&&ByRefUserID = jxg9620 %then %do;
  %let &&ByRefPassword = {SAS002}4A77E11954BA7AA94F40A74A5B26082C;
%end;
%if &Type = Personal and &&&ByRefUserID = axa9815 %then %do;
  %let &&ByRefPassword = {sas002}56AFFB055CB223230A8A277F5158D54E;
%end;
%if &Type = Personal and &&&ByRefUserID = yta0527 %then %do; /*tony*/
  %let &&ByRefPassword = {SAS002}670D353041B9A1A353922DDE26945C23;
%end;
%if &Type = Personal and &&&ByRefUserID = ird8431 %then %do;
  %let &&ByRefPassword = {sas002}C4A1204846758E1A57FF782C0394B5DC;
%end;
%if &Type = Personal and &&&ByRefUserID = zmh1459 %then %do;
  %let &&ByRefPassword = {sas002}4BD7261746DA666C4D947EB402BACFC6;
%end;
%if &Type = Personal and &&&ByRefUserID = dng6340 %then %do;
  %let &&ByRefPassword = {sas002}941BF82F28111BD71FAEB9443CF2768E;
%end;

%else %if &System = Oracle and &Type = Standard %then %do;
  %let &&ByRefUserID = sb_sandbox_actuary;
  %let &&ByRefPassword = {SAS002}BA7B9D0627AB487D3E492BEC409A3E46;
%end;
%else %if &System = SalesInformatics or &System = Spectrum %then %do;
  %let &&ByRefUserID = Informatics_BP_User;
  %let &&ByRefPassword = {sas002}5A38373E39E6F26558B148DD55535A8316DE2248;  
%end;
%else %if &System = SalesAnalyticsProd and &Type = Standard %then %do;
  %let &&ByRefUserID = H1SalAnl;
  %let &&ByRefPassword = {sas002}FB97993B2C693DA504A893802906C7A62D13E4C7;  
%end;
%else %if &System = SalesAnalyticsQA and &Type = Standard %then %do;
  %let &&ByRefUserID = H1SalAnl;
  %let &&ByRefPassword = {sas002}99906B3625F591165AFE276D2F16B6204D2B2AE1;  
%end;
%else %if &System = SalesAnalyticsTest and &Type = Standard %then %do;
  %let &&ByRefUserID = H1SalAnl;
  %let &&ByRefPassword = {sas002}84050D42038508C108997A06269995E230602CF1;  
%end;
%else %if &System = H1 %then %do;
  %let &&ByRefUserID = H1FinRpt;
  %let &&ByRefPassword = {SAS002}EB62BD0355B19D963E063BB1434AF046;  
%end;
%if &Type = Personal and &&&ByRefUserID = bms9883 %then %do;
  %let &&ByRefPassword = {SAS002}39AAD23E55893BE32A225033455436BE;
  
%end;




/*Encryption Code*/
/*PROC PWENCODE IN='xxxx' METHOD=sas002;RUN;*/

%mend;
