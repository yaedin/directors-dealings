# Directors' Dealings

Wouldn't it be great to have deep insights of a company before you invest in it? Be keenly aware of its strategic vision and major decision points in the future? Directors of a company (think C-Level, Board members, etc.) are in possession of such information.

Which is fine on its own. But what would happen, if a director wants to buy shares of the company he or she has such exclusive insights of? Wouldn't that be an unfair advantage? 

In order to prevent market manipulation and insider trading it is mandatory in Germany to disclose relevant transactions. This project uses a database from the [BaFin](https://www.bafin.de/DE/PublikationenDaten/Datenbanken/DirectorsDealings/directorsdealings_node.html) (Bundesanstalt für Finanzdienstleistungsaufsicht - German Federal Financial Supervisory Authority) to list and analyze such directors' dealings, as they are called. 

I was inspired about this phenomenon by the guys from [Quiver Quantitative](https://www.quiverquant.com/sources/senatetrading) particularly their senator trading view, obviously also a group of people with access to information other people do not have.

If you have any further questions or feedback, feel free to write me at yasin.edin@outlook.de

# Demo
![Demo](demo.gif)

# How does this work

After loading the app, you can select or type a company name. Currently there are over 1.100 companies present in the database. In the next step, historical stock prices and respective director returns are loaded and visualized. Please note that the director dealings ("buys" or "sells") you see on my app represent the **lower limit** of a directors dealing with this company. This has a few reasons:

  1. Only trades with an aggregated volume of 5.000 EUR or more are included (the BaFin did increase this reporting threshold to 20.000 EUR on the 1st January of 2020, but I as I can relate more with 5.000 EUR, I left it as is :)
  2. Some entries in the BaFin are not fully disclosed. So for example share matchings are not fully disclosed in the BaFin - e.g. there is no information regarding price and volume present or it is very difficult to parse - and are therefore excluded.
  3. Currently the database has entries up to early 2019. This can lead to situations where I have leading sells or exclusive sells from directors. For return calculation purposes I ascribe each director a virtual account and therefore exclude these leading or exclusive sells as well (although I still display them in the chart).
  
For dealings, which are left, you will find for each director or member of the board a description of the total account value, the return on investment as a nominal figure and as a percentage. In cases where there was a "buy" event and the stock has not been sold yet, I calculate return values based on the current stock prize. 

# My learnings

  1. Research:
    Most simply, I learned about Directors' Dealings in Germany, their level of regulation and thus the level of transparency for this data.
  2. Techincal:
    I learned about how features can maskerade as bugs, that clean data is a proxy for trust. I learned that frontend and backend should have seperate lifecycles and the former should be kept as simple as possible.
    Regarding Frameworks: I learned about selenium. Vastly improved my docker skills and expanded my skills in PostgreSQL, FastAPI and Streamlit.

# Technical Instructions

If you want to deploy a local version, the easiest way would be to do so via docker. Afterwards you will need to run `docker-compose up`. To populate the database you will need to further execute at least once `get_trades_bafin.py` and `db_init.py` within the code directory (which will exptect a `data` directory to be there). 
Please be advised that would you will need to create your own username and password for the databse in `docker-compose` file and the various scripts.

You then can:
  1. Interact directly with the app through [streamlit](https://www.streamlit.io/) through your localhost on the port 8501.
  2. Query data through the FastAPI through the port 8080.
  3. (optional) Run Jupyter Lab for development purposes.
