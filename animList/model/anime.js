export default class Anime {

    _id;

    _imageUrl; // dico contenant les images

    // les différents titres
    _titles;

    _type; //movie series TV ...
    _nbEpisode; // le nombre d'épisodes total
    _duration; // temps d'un épisode

    //différents score
    _rank;
    _rating; // age pour regarder
    _popularity;

    _synopsis;
    _year; //année sortie
    _season; //saisson de la sortie

    //genres
    _genres;
    _explicitGenres;

    //production de l'anime
    _studios;
    _producers;
    _licensors;

    //diffusion
    _status;
    _airing;
    _aired;

    constructor(animData) { //for (let key in editableBtns)
        this._id = animData.mal_id;
        this._imageUrl = animData.images.jpg;
        this._titles = animData.titles;
        this._type = animData.type;
        this._nbEpisode = animData.episode;
        this._duration = animData.duration;
        this._rank = animData.rank;
        this._rating = animData.rating;
        this._popularity = animData.popularity;
        this._synopsis = animData.synopsis;
        this._year = animData.year;
        this._season = animData.season;
        this._genres = animData.genres;
        this._explicitGenres = animData.explicitGenres;
        this._studios = animData.studios;
        this._producers = animData.producers;
        this._licensors = animData.licensors;
        this._status = animData.status;
        this._airing = animData.airing;
        this._aired = animData.aired;
      }
}