class API {

    _urlApi = 'http://api.jikan.moe/v4/';
    _sfw = '&sfw';
    _headURL = 'https://api.allorigins.win/get?url=';

    constructor(){}

    searchAnimeByText(input){
        return fetch(this._headURL + encodeURIComponent(this._urlApi + 'anime?q=' + input + this._sfw));
    }

    searchAnimeById(id){
        return fetch(this._headURL + encodeURIComponent(this._urlApi + 'anime/' + id));
    }

}