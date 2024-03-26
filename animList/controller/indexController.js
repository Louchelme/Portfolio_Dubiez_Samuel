//import Anime
//import create_index_card

/**
 * Objet constant représentant la vue.
 */
const view = {

	//Barre de recherche
	searchBar: document.querySelector("#searchBar"),

	//div contenant les cards
	listAnime: document.querySelector('#listAnime'),

	//Loading bar
	loadingBar: document.querySelector('#loading-bar'),

	//Le caroussel
	caroussel: document.querySelector('#carousselTopAnime'),

};


//initialisation ---
let api = new API();
Anime.restoreState();

/**
 * Chargement des top animes
 */
api.retrieveTopAiringAnime()
	.then(async (response) => {
		let recherche = await response.json();
		let data = JSON.parse(recherche.contents).data;

		data.forEach(elem => {
			//convertion des données en objet Anime
			let anime = new Anime(elem)

			//création de la 'card'
			let card = create_item_caroussel(anime);
			view.caroussel.appendChild(card);

			card.querySelector('.img-caroussel').addEventListener('click', (evt) => {
				document.location.href = './view/anime.html?id=' + anime.getId();
			})
		});
	})


/**
 * Set-up des Listeners
 */


// --- Barre de recherche ---
view.searchBar.addEventListener("change", async (evt) => {
	if (evt.target.value.trim().length === 0) {
		return
	}

	if (document.querySelector(".carousel")) {
		document.querySelector("#topRank").removeChild(document.querySelector(".carousel"));
		document.querySelector("#topRank").children[0].innerText = "Résultat de la recherche";
	}

	// -- Suppression de l'affichages des anciens animes
	view.listAnime.innerHTML = "";
	// -- desactivation de la bar de recherche
	view.searchBar.setAttribute('disabled', 'disabled');
	// affichage de la bar de chargement
	view.loadingBar.removeAttribute('hidden');

	//envoie de la requéte api et attente du résultat
	api.searchAnimeByText(evt.target.value)
		.then(async (response) => {

			let recherche = await response.json();
			let data = JSON.parse(recherche.contents).data;

			data.forEach(elem => {
				//convertion des données en objet Anime
				let anime = new Anime(elem)

				//création de la 'card'
				let card = create_index_card(anime);
				view.listAnime.appendChild(card);

					
				//Ajout du click sur l'image de la card
				card.querySelector('.card-img-top').addEventListener('click', () => {
					document.location.href = './view/anime.html?id=' + anime.getId();
				});


				let btnAddFav = card.querySelector(".btn-favory");


				if (Anime.getFavoris()[anime.getId()]) {
					btnAddFav.remove();
					return;
				}


				btnAddFav.addEventListener('click', (evt) => {
					Anime.addFavori(anime);
					//supression du boutton ajout favoris
					evt.target.remove();
				});
			});

			//réactivation de la barre de recherche
			view.searchBar.removeAttribute('disabled');
			//désaffichage de la barre de chargement
			view.loadingBar.setAttribute('hidden', '');

			//affichage d'un message si aucun résultat trouvé
			if (data.length === 0) {
					view.listAnime.innerText = "Aucun anime trouvé :c ";
			}

		});
});
