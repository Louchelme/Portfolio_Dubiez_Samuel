class Anime {

    _id;

    _dicoImageUrl; // dico contenant les images

    // les différents titres
    _dicoTitles;

    _type; //movie series TV ...
    _duration; // temps d'un épisode

    //différents score
    _rank;
    _rating; // age pour regarder
    _popularity;

    _synopsis;
    _year; //année sortie
    _season; //saisson de la sortie

    //genres
    _tabGenres = [];
    _tabExplicitGenres = [];

    //production de l'anime
    _tabStudios;
    _tabProducers;
    _tabLicensors;

    //diffusion
    _status;
    _airing;
    _dicoAired;

    //nb Episodes vue
    _curentEpisode;
    _totalEpisode; // le nombre d'épisodes total

    constructor(animData) { //for (let key in editableBtns)
        this._id = animData.mal_id;
        this._ImageUrl = animData.images.jpg.large_image_url; 
        this._dicoTitles = animData.titles; 
        this._type = animData.type;
        this._totalEpisode = animData.episode;
        this._duration = animData.duration;
        this._rank = animData.rank;
        this._rating = animData.rating;
        this._popularity = animData.popularity;
        this._synopsis = animData.synopsis;
        this._year = animData.year;
        this._season = animData.season;

        for(let [id, value] in animData.genres){
            this._tabGenres[id] = animData.genres[id].name;
        }

        for(let [id, value] in animData.explicitGenres){
            this._tabExplicitGenres[id] = animData.explicitGenres[id].name;
        }

        for(let [id, value] in animData.studios){
            this._tabStudios[id] = animData.studios[id].name;
        }

        for(let [id, value] in animData.producers){
            this._tabProducers[id] = animData.producers[id].name;
        }

        for(let [id, value] in animData.licensors){
            this._tabLicensors[id] = animData.licensors[id].name;
        }

        this._status = animData.status; 
        this._airing = animData.airing;
        this._dicoAired = {'from':animData.aired.from, 'to' :animData.aired.to};
      } 


    getCurrentEpisode() {
        return this._curentEpisode ?? 0;
    }

    setCurentEpisode(idEpisode) {
        this._curentEpisode = idEpisode;
    }
}