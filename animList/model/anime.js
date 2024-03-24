class Anime {

    static favoris = {};

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

    /**
     * Créer une entité Anime a partir des données de l'API envoyé en argument.
     * @param {*} animData donnée brute d'un animé envoyé par l'api
     */
    constructor(animData) {
        if(animData.mal_id == undefined){
            Object.assign(this, animData);
            return;
        }

        this._curentEpisode = 0;

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

        for(let id in animData.genres){
            this._tabGenres[id] = animData.genres[id].name;
        }

        for(let id in animData.explicitGenres){
            this._tabExplicitGenres[id] = animData.explicitGenres[id].name;
        }

        for(let id in animData.studios){
            this._tabStudios[id] = animData.studios[id].name;
        }

        for(let id in animData.producers){
            this._tabProducers[id] = animData.producers[id].name;
        }

        for(let id in animData.licensors){
            this._tabLicensors[id] = animData.licensors[id].name;
        }

        this._status = animData.status;

        //init _dicoAired
        let to = '';
        let from = '';
        if(animData.aired.to == null ){
            to = 'non finie';
        } else {
            to = animData.aired.to.substr(0,10) ?? 'pas encore finie';
        }
        if(animData.aired.from == null ){
            from = 'non commancé';
        } else {
            from = animData.aired.from.substr(0,10) ?? 'pas encore finie';
        }
        this._dicoAired = {'from':from, 'to' :to};
      } 

    /**
     * R'envoie les favoris.
     * @returns un objet sous la forme d'une liste de {'<id>': <Objet Anime>}
     */
    static getFavoris(){
        return Anime.favoris;
    }

    /**
     * Ajout un Anime a la variable static 'favoris'.
     * @param {*} anime un objet Anime
     */
    static addFavori(anime){
        if (Anime.favoris[anime.getId()]){
            Anime.deleteFavori(anime);
            console.log('passage dans delete ');
        }
        Anime.favoris[anime.getId()] = anime;
        console.log(Anime.favoris);
        Anime.saveState()
    }

    /**
     * Supprime l'objet Anime correspondant de la variable static 'favoris'.
     * @param {*} anime un objet Anime
     */
    static deleteFavori(anime){
        delete Anime.favoris[anime.getId()];
        Anime.saveState()
    }

    /**
     * R'envoie le numéro d'épisode en cours de visionage de la série.
     * @returns une String
     */
    getCurrentEpisode() {
        return this._curentEpisode ?? 0;
    }

    /**
     * Change le numéro de l'épisode.
     * @param {*} numEpisode un String
     */
    setCurrentEpisode(numEpisode) {
        this._curentEpisode = numEpisode;
        Anime.addFavori(this);

    }

    /**
     * R'envoie le titre par défaut.
     * @returns un String
     */
    getDefaultTitle(){
        return this._dicoTitles[0].title;
    }

    /**
     * R'envoie l'identifiant de la série.
     * @returns un String
     */
    getId(){
        return this._id;
    }

    /**
     * R'nevoie le titre de la langue spécifié ou le titre par défaut si il y en a pas.
     * @param {*} langue la langue écrit en format 'French'
     * @returns un String
     */
    getTitle(langue){
        for(var key in this._dicoTitles){
            if(this._dicoTitles[key].type == langue) return this._dicoTitles[key].title;
        }
        return this.getDefaultTitle();
    }

    /**
     * R'envoie l'URL de l'image de présentation de la série.
     * @returns un String
     */
    getImageURL(){
        return this._imageUrl;
    }

    /**
     * R'envoie les 200 1er caractéres du synopsis suivit de '[...]'.
     * @returns un String
     */
    getShortSynopsis(){
        if (!this._synopsis) {
            return '';
        }
        if (this._synopsis.length <= 50*4 ) {
            return this._synopsis;
        }
        return this._synopsis.slice(0, 50*4) + ' [...]' ?? '';
    }

    /**
     * R'envoie le synopsis en entier.
     * @returns un String
     */
    getFullSynopsis(){
        return this._synopsis ?? '';
    }

    /**
     * R'envoie le nombre d'épisode de la série.
     * @returns un String
     */
    getNumberTotalOfEpisode(){
        return this._totalEpisode ?? 0;
    }

    /**
     * R'envoie le support de la série (TV, anime, manga, ...).
     * @returns un String
     */
    getType(){
        return this._type ?? '';
    }

    /**
     * R'envoie la durée moyenne d'un épisode de la série.
     * @returns un String
     */
    getDuration(){
        return this._duration ?? '';
    }

    /**
     * R'envoie le rank de la série.
     * @returns un String
     */
    getRank(){
        return this._rank ?? '';
    }

    /**R'envoie l'age minimum pour ragarder la série.
     * 
     * @returns un String
     */
    getRating(){
        return this._rating ?? 'unknow';
    }

    /**
     * R'envoie la popularité de la série
     * @returns un String
     */
    getPopularity(){
        return this._popularity ?? 'unknow';
    }

    /**
     * R'envoie l'année de sortie de la série.
     * @returns un String
     */
    getYear(){
        return this._year ?? 'unknow';
    }

    /**
     * R'envoie la saison de sortie de la série.
     * @returns un String
     */
    getSeason(){
        return this._season ?? 'unknow';
    }

    /**
     * R'envoie la liste des genres de la série.
     * @returns un tableau de String : [String]
     */
    getGenres(){
        return this._tabGenres ?? ['unknow'];
    }

    /**
     * R'envoie la liste des genres explicites de la série.
     * @returns un tableau de String : [String]
     */
    getExpliciteGenres(){
        return this._tabExplicitGenres ?? [];
    }

    /**
     * R'envoie la liste des studios de la série.
     * @returns un tableau de String : [String]
     */
    getStudios(){
        return this._tabStudios ?? ['unknow'];
    }

    /**
     * R'envoie la liste des producteurs de la série.
     * @returns un tableau de String : [String]
     */
    getProducers(){
        return this._tabProducers ?? ['unknow'];
    }
    
    /**
     * R'envoie la liste des licenciers de la série.
     * @returns un tableau de String : [String]
     */
    getLicensors(){
        return this._tabLicensors ?? ['unknow'];
    }

    /**
     * R'envoie le status de sortie de la série.
     * @returns un String
     */
    getStatus(){
        return this._status ?? '';
    }

    /**
     * R'envoie la dates de début de sortie et de fin de sortie sous format 'aaaa-mm-jj'.
     * @returns un objet {'from': String, 'to': String}
     */
    getAired(){
        return this._dicoAired ?? {};
    }

    /**
     * Sauvegarde la variable 'favoris' dans le localStorage.
     */
    static saveState(){
        //conversion des favoris en json
        var state = JSON.stringify(Anime.favoris);
        //sauvegarde dans le local storage
        localStorage.setItem("favoris", state);
    }

    /**
     * set la variable 'favoris' avec le contenue du localStorage.
     */
    static restoreState(){
        //récupération des favoris du local storage
        let state = localStorage.getItem("favoris");
        let data = JSON.parse(state);

        for(let id in data){
            Anime.favoris[id] = new Anime(data[id]);
        }
    }
}