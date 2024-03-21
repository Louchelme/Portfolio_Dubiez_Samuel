class Anime {

    _id;
    _imageUrl;

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
    _tabStudios = [];
    _tabProducers = [];
    _tabLicensors = [];

    //diffusion
    _status;
    _airing;
    _dicoAired;

    //nb Episodes vue
    _curentEpisode;
    _totalEpisode; // le nombre d'épisodes total

    constructor(animData) { //for (let key in editableBtns)
        this._id = animData.mal_id;
        this._imageUrl = animData.images.jpg.large_image_url; 
        this._dicoTitles = animData.titles; 
        this._type = animData.type;
        this._totalEpisode = animData.episodes;
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

    getDefaultTitle(){
        return this._dicoTitles[0].title;
    } 

    /**
     * 
     * @param {*} langue la langue écrit en format 'French'
     */
    getTitle(langue){
        for(let [key, value] in this._dicoTitles){
            if(value.type == langue) return value.title;
        }
        return this._dicoTitles[0].title;
    }

    getImageURL(){
        return this._imageUrl;
    }
    getShortSynopsis(){
        if (!this._synopsis) {
            return '';
        }
        if (this._synopsis.length <= 50*4 ) {
            return this._synopsis;
        }
        return this._synopsis.slice(0, 50*4) + ' [...]' ?? '';
    }

    getFullSynopsis(){
        return this._synopsis ?? '';
    }

    getNumberTotalOfEpisode(){
        return this._totalEpisode ?? 0;
    }

    getType(){
        return this._type ?? '';
    }

    getDuration(){
        return this._duration ?? '';
    }

    getRank(){
        return this._rank ?? '';
    }

    getRating(){
        return this._rating ?? 'unknow';
    }

    getPopularity(){
        return this._popularity ?? 'unknow';
    }

    getYear(){
        return this._year ?? 'unknow';
    }

    getSeason(){
        return this._season ?? 'unknow';
    }

    getGenres(){
        return this._tabGenres ?? ['unknow'];
    }

    getExpliciteGenres(){
        return this._tabExplicitGenres ?? [];
    }

    getStudios(){
        return this._tabStudios ?? ['unknow'];
    }

    getProducers(){
        return this._tabProducers ?? ['unknow'];
    }
    
    getLicensors(){
        return this._tabLicensors ?? ['unknow'];
    }

        //diffusion
        _status;
        _airing;
        _dicoAired;

    getStatus(){
        return this._status ?? '';
    }

    getAiring(){
        return this._airing ?? '';
    }

    getAired(){
        return this._dicoAired ?? {};
    }
}