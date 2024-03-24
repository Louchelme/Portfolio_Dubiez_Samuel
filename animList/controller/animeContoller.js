//import Anime
//import create_index_card

/**
 * Objet constant représentant la vue.
 */
const view = {

	//titles
	animeTitles: document.querySelector("#titles"),

	//image
	animeImg: document.querySelector('#image'),

	//rank
	animeRank: document.querySelector('#rank'),

	//popularity
	animePopularity: document.querySelector('#popularity'),

	//type
	animeType: document.querySelector('#type'),

	//rating
	animeRating: document.querySelector('#rating'),

	//genres
	animeGenres: document.querySelector('#genres'),

	//status
	animeStatus: document.querySelector('#status'),

	//number of episode
	animeNbEpisde: document.querySelector('#nb_episode'),

	//duration
	animeDuration: document.querySelector('#duration'),

	//sortie
	animeSortie: document.querySelector('#sortie'),

	//aired
	animeAired: document.querySelector('#aired'),

	//studio
	animeStudio: document.querySelector('#Studio'),

	//producters
	animeProducters: document.querySelector('#producters'),

	//licensers
	animeLicensers: document.querySelector('#licensers'),

	//synopsis
	animeSysnopsis: document.querySelector('#synopsis'),

	//btn favori
	favori: document.querySelector('#favori'),

};

//initialisation ---
let api = new API();
let anime;

//récupération de l'identifiant de l'animé dans l'url
let params = new URL(document.location).searchParams;
let id = params.get("id");

//favori
let isFavori;
let delFavori = 'Retirer des favoris';
let addFavori = 'Ajouter au favoris';

/**
 * Envoie de la requéte api et affichage du résultat
 */
api.searchAnimeById(id).then(async (response) => {

	//traitement de la réponse api
	let recherche = await response.json();
	let data = JSON.parse(recherche.contents).data;

	//conversion en un objet Anime
	anime = new Anime(data);

	//placement des informations dans la page
	view.animeImg.src = anime.getImageURL(); //image
	view.animeTitles.innerText = anime.getTitle("English"); //title

	//popularity
	if(anime.getPopularity() == ''){
		view.animeRank.innerText = 'Score: None';
	} else {
		view.animePopularity.innerText = 'Score: ' + anime.getPopularity();
	}

	//rank
	if(anime.getRank() == ''){
		view.animeRank.innerText = 'Rank: None';
	} else {
		view.animeRank.innerText = 'Rank: ' + anime.getRank();
	}

	view.animeType.innerText = anime.getType();//type
	view.animeRating.innerText = anime.getRating();//rating

	//genres
	let listGenres = '';
	let genres = anime.getGenres();
	for(let id in genres){
		listGenres += genres[id] + ', ';
	}
	view.animeGenres.innerText = listGenres;

	view.animeNbEpisde.innerText = anime.getNumberTotalOfEpisode();//number of episode
	view.animeDuration.innerText = anime.getDuration();//duration
	view.animeSortie.innerText = anime.getYear() + ' ' + anime.getSeason();//sortie
	view.animeStatus.innerText = anime.getStatus();//status

	view.animeAired.innerText = 'de ' + anime.getAired().from + "\nà " + anime.getAired().to //aired

	//studio
	let listStudio = '';
	let studios = anime.getStudios();
	for(let id in studios){
		listStudio += studios[id] + ', ';
	}
	view.animeStudio.innerText = listStudio;

	//producters
	let listProducters ='';
	let producters = anime.getProducers();
	for(let id in producters){
		listProducters += producters[id] + ', ';
	}
	view.animeProducters.innerText = listProducters;

	//licensers
	let listLicesers = '';
	let licensers = anime.getStudios();
	for(let id in licensers){
		listLicesers += licensers[id] + ', ';
	}
	view.animeLicensers.innerText = listLicesers;

	view.animeSysnopsis.innerText = anime.getFullSynopsis();//synopsis
		
	///initialisation du boutton favori
	if(Anime.getFavoris()[anime.getId()] == 'undefined'){
		view.favori.innerText = delFavori;
		isFavori = true;
	} else {
		view.favori.innerText = addFavori;
		isFavori = false;		
	}

});

/**
 * listener boutton favori
 */
favori.addEventListener('click', (evt) => {
	if(isFavori){
		Anime.deleteFavori(anime)
		isFavori = false;
		view.favori.innerText = addFavori;
	} else {
		Anime.addFavori(anime);
		isFavori = true;
		view.favori.innerText = delFavori;
	}
});
