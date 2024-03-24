
/**
 * Créé une card VERTICAL avec le titre, l'image et bout du résumé
 * @param {Anime} anime
 * @returns Retourne la card créé a partir de l'anime reçu
 */
function create_index_card(anime) {

	// --- Creation de la Main div ---
	let mainDiv = document.createElement('div');
	mainDiv.classList.add('col');

	//Creation de la card
	let cardDiv = document.createElement('div');
	cardDiv.classList.add('card');
	cardDiv.classList.add('shadow-sm');
	mainDiv.appendChild(cardDiv);

	// --- Creation de l'image ---
	let imgCard = document.createElement('img');
	imgCard.style.objectFit = 'cover';
	imgCard.src = anime.getImageURL();
	imgCard.classList.add('card-img-top');
	imgCard.height = 450;
	imgCard.draggable = false;
	cardDiv.appendChild(imgCard);

	// --- Creation de la div card-body ---
	let cardBody = document.createElement('div');
	cardBody.classList.add('card-body');
	cardDiv.appendChild(cardBody);

	// --- Creation du Titre ---
	// TODO : add rank quelque part ?
	let titreCard = document.createElement('h4');
	titreCard.classList.add('card-title');
	titreCard.innerText = anime.getDefaultTitle();
	cardBody.appendChild(titreCard);

	// --- Creation du resumé ---
	let resumeCard = document.createElement('p');
	resumeCard.classList.add('card-text');
	resumeCard.innerText = anime.getShortSynopsis();
	cardBody.appendChild(resumeCard);

	// zone Boutton
	let containerButton = document.createElement('div');
	containerButton.classList.add('card-body');
	cardBody.appendChild(containerButton);


	// --- Creation Boutton Voir plus ---
	let buttonVoirCard = document.createElement('a')
	buttonVoirCard.classList.add('btn');
	buttonVoirCard.classList.add('btn-dark');
	buttonVoirCard.innerHTML = 'Voir +';
	buttonVoirCard.href = 'view/anime.html?id=' + anime._id;
	containerButton.appendChild(buttonVoirCard);

	// --- Creation Boutton ajouter au favoris ---
	let buttonAddFavCard = document.createElement('a')
	buttonAddFavCard.classList.add('btn');
	buttonAddFavCard.classList.add('btn-danger');
	buttonAddFavCard.innerHTML = 'Ajouter au favoris <i class="bi bi-suit-heart-fill"></i>';
	containerButton.appendChild(buttonAddFavCard);

	return mainDiv;


	/*
	<a href="#" class="btn btn-danger">Ajouter au favoris <i class="bi bi-suit-heart-fill"></i></a>
	*/
}

/**
 * Créé une card HORIZONTAL avec à gauche l'image (et une icone coeur) et à droite le titre + un bout du résumé
 * @param {Anime} anime
 * @returns Retourne la card créé a partir de l'anime reçu
 */
function create_favory_card(anime) {

	// --- Creation de la main div ---
	let rootElement = document.createElement('div');
	rootElement.classList.add('card');
	rootElement.classList.add('mb-3'); //margin bottom de 3

	rootElement.innerHTML = 
	`<div class="row">
		<div class="col-md-2">
			<img src="${anime.getImageURL()}" class="card-img" />
		</div>
		<div class = "col-md-8">
			<div class="card-body">
				<h5 class ="ard-title">${anime.getDefaultTitle()} </h5>
				<p class="card-text">
					<p>Current episode : </p>
					<div class="input-group">
						<input type="number" class="form-control" value="$anime.anime.getCurrentEpisode()" />
						<div class ="input-group-append">
							<span class="input-group-text">/ ${anime.getNumberTotalOfEpisode()}</span>
						</div>
					</div>
				</p>
			</div>
		</div>
		<div class="col-md-2 mx-auto justify-content-md-end">
			<a class="btn btn-danger btn-favory" role="button">Remove <i class="bi bi-heartbreak"></i></a>
		</div>
	<div>
	`;
	return rootElement;

}

function create_item_caroussel(anime) {
	// <div class="carousel-item">
	// 	<img src="..."
	// 		class="d-block w-100" alt="...">
	// </div>
	let rootElement = document.createElement('div');
	rootElement.classList.add('carousel-item');

	let img = document.createElement('img');
	img.src = anime.getImageURL();
	img.classList.add("img-caroussel");
	img.classList.add("d-block")
	img.classList.add("w-100")
	rootElement.appendChild(img);

	return rootElement;
}