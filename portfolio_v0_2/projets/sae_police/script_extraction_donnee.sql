/*=====================================================================================*
Réalisé par Samuel Dubiez et Romain Quincieu
 *=====================================================================================*/

/***************************************************************************************
  Nettoyage de la base de donnée de la police.
 ***************************************************************************************/

 CREATE temp VIEW caracteristiques AS
 SELECT
    num_acc,
    make_date(an, mois, jour) as jour_cal,
    an::int  as an,
    mois,
    jour,
    to_char(make_date(an,mois,jour), 'Day') as jour_sem,
    hrmn,
    to_char(hrmn,'am') as mi_jour,
    lum,
    dep,
    com,
    agg,
    int,
    atm,
    col,
    adr
 FROM caracteristiques_2019
  union
   SELECT
    num_acc,
    make_date(an, mois, jour) as jour_cal,
    (an + 2000)::int as an,
    mois,
    jour,
    to_char(make_date(an,mois,jour), 'Day') as jour_sem,
    cast(lpad(cast(hrmn as text),4,'0') as time) as hrmn,
    to_char(cast(lpad(cast(hrmn as text),4,'0') as time),'am') as mi_jour,
    lum,
    ltrim(left(replace(replace(dep,'201','2A0'),'202','2B0'),2),'0') || rtrim(right(replace(replace(dep,'201','2A0'),'202','2B0'),1),'0') as dep,
    com,
    agg,
    int,
    atm,
    col,
    adr
 FROM caracteristiques_2006_2018
 ;
 
 /************************************************************/
 
 CREATE temp VIEW lieux AS
 SELECT num_acc, catr, nbv, vosp, prof, surf, infra, situ
 FROM lieux_2006_2018
   UNION
 SELECT num_acc, catr, nbv, vosp, prof, surf, infra, situ
 FROM lieux_2019;
    
/************************************************************/
 
 CREATE temp TABLE vehicules AS
 SELECT num_acc, num_veh, catv, obs, obsm, choc, occutc
 FROM vehicules_2019
  UNION
 SELECT num_acc, num_veh, catv, obs, obsm, choc, occutc
 FROM vehicules_2006_2018;
 
 UPDATE vehicules SET catv = 'velo' WHERE catv = '01';
 UPDATE vehicules SET catv = 'cyclo_leger' WHERE catv = '02' or catv = '30';
 UPDATE vehicules SET catv = 'voiture' WHERE catv = '07';
 UPDATE vehicules SET catv = 'utilitaire' WHERE catv = '10';
 UPDATE vehicules SET catv = 'poid_lourd' WHERE catv = '13' or catv = '14' or catv = '15' or catv = '16' or catv = '17';
 UPDATE vehicules SET catv = 'tracteur' WHERE catv = '21';
 UPDATE vehicules SET catv = 'cyclo_puissant' WHERE catv = '31' or catv = '32' or catv = '33' or catv = '34';
 UPDATE vehicules SET catv = 'bus' WHERE catv = '37';
 UPDATE vehicules SET catv = 'car' WHERE catv = '38';
 UPDATE vehicules SET catv = 'train' WHERE catv = '39';
 UPDATE vehicules SET catv = 'tram' WHERE catv = '40';
 UPDATE vehicules SET catv = 'trott_moteur' WHERE catv = '50';
 UPDATE vehicules SET catv = 'trottinette' WHERE catv = '60';
 UPDATE vehicules SET catv = 'velo_elec' WHERE catv = '80';
 UPDATE vehicules SET catv = 'autre' WHERE catv = '__';
 
 /************************************************************/
 
 CREATE TEMP VIEW usagers (num_acc, 
    num_veh,
           place, 
           catu, 
           grav, 
           sexe,
           an_nais,
           trajet,
           secu1,
           secu2,
           secu3,
           etatp) 
as select upre2019.num_acc,
    upre2019.num_veh, 
    upre2019.place,
           upre2019.catu,
           upre2019.grav,
           upre2019.sexe,
           upre2019.an_nais,
           upre2019.trajet,
           regexp_replace(regexp_replace(regexp_replace(upre2019.secu,'.[03]',' -1'),'.2','0'),'^(.)1','1') as secu1, 
    null as secu2,
        null as    secu3,
           upre2019.etatp
from usagers_2006_2018 upre2019
union
select upost2019.num_acc,
    upost2019.num_veh,
    upost2019.place,
    upost2019.catu,
    upost2019.grav,
    upost2019.sexe,
    upost2019.an_nais,
    upost2019.trajet,
    upost2019.secu1,
    upost2019.secu2,
    upost2019.secu3,
    upost2019.etatp
from usagers_2019 upost2019;
 
 
/***************************************************************************************
  Extraction et Transformation des données utile.
 ***************************************************************************************/

  CREATE TEMP TABLE usagers_paca_jeunes AS 
    SELECT u.*, c.an - u.an_nais as age
    FROM usagers u 
    JOIN caracteristiques c ON c.num_acc = u.num_acc
    WHERE (c.dep = '4' OR c.dep = '5' OR c.dep = '6' OR c.dep = '13' OR c.dep = '83' OR c.dep = '84') and 18 < (c.an - u.an_nais) and (c.an - u.an_nais) < 24;

  CREATE TEMP TABLE vehicules_paca_jeunes AS 
    SELECT v.* 
    FROM vehicules v 
    NATURAL JOIN usagers_paca_jeunes u;

  CREATE TEMP TABLE caracteristique_lieux_paca_jeunes AS
    SELECT c.*, l.catr, l.nbv, l.vosp, l.prof, l.surf, l.infra, l.situ, d.nom, d.population, 
    (SELECT count(v.num_veh) AS nb_veh FROM vehicules_paca_jeunes v WHERE v.num_acc = c.num_acc GROUP BY v.num_acc ),
    (SELECT count(u.place) AS nb_vic FROM usagers_paca_jeunes u WHERE u.num_acc = c.num_acc GROUP BY u.num_acc ),
    (SELECT (count(u.place) > 0) AS mortel FROM usagers_paca_jeunes u WHERE u.num_acc = c.num_acc AND u.grav = '2')
    FROM ((caracteristiques c NATURAL JOIN lieux l) LEFT JOIN departements_francais d ON c.dep = d.num)
    ;

  CREATE TEMP TABLE bilan_departements_paca_jeunes AS
    SELECT c.dep, c.an,c.nom,
      count(c.num_acc) AS nb_acc,
      (count(c.num_acc)/ c.population) AS nb_acc_hab,
      (SELECT count(c.num_acc)/count(ca.num_acc) FROM caracteristique_lieux_paca_jeunes ca WHERE an = 2006 AND (c.dep, c.an) = (ca.dep, ca.an) GROUP BY (c.an,c.dep,c.nom,c.population) ) AS nb_acc_r_2006
    FROM  caracteristique_lieux_paca_jeunes c
    GROUP BY (c.an,c.dep,c.nom,c.population)
    LIMIT 10;
    


/***************************************************************************************
  Exportation en format CSV des données utile.
 ***************************************************************************************/

  \copy usagers_paca_jeunes to '~/S2.04/usagers_paca_jeunes.csv' CSV HEADER NULL 'NA' ENCODING 'UTF-8' DELIMITER E'\t;  
  \copy usagers_paca_jeunes to '~/S2.04/vehicules_paca_jeunes.csv' CSV HEADER NULL 'NA' ENCODING 'UTF-8' DELIMITER E'\t;
  \copy usagers_paca_jeunes to '~/S2.04/caracteristique_lieux_paca_jeunes.csv' CSV HEADER NULL 'NA' ENCODING 'UTF-8' DELIMITER E'\t;
  \copy usagers_paca_jeunes to '~/S2.04/bilan_departements_paca_jeunes.csv' CSV HEADER NULL 'NA' ENCODING 'UTF-8' DELIMITER E'\t;
