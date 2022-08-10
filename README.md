# Power-Europe

Feuille de route:

Markit:
- recuperer les donnes de markit: voir fichier crystal et le lien xml
- determiner la moyenne de la std sur chaque indice / maturite
- determiner un threshold pour la validation des prix

Extrapolation:
- pour les indices avec suffisamment de prix, determiner une maniere de recontruire la forward curve avec un set predetermine et constant de valeurs a renseigner manuellement (mois, quarter,annee en BL/PKS)

Curve D avec D-1:
- determiner un modele pour creer une forward curve avec celles du passe: etudier question des croissances moyennes ou des RNN

--------------------------------------------------------------------------------------------------------------------------------------------------------
Planning retrospectif:

08/08: creation du repo partage
09/08: creation du module mod (avec les fonctions persos), recuperation des consensus markit a partir du folder W:\UK\Risk_Control\Risk_Control_Private\MiddleOffice\GPE\Prices\Price Check Tool\Export\Mark-It Raw,

- donnes markit ok avril

10/08: donnees csv correctes, calcul std dev moyenne sur chaque maturit/index, run le script pour les mois restants


