
/**
 * Créé une card VERTICAL avec le titre, l'image et bout du résumé
 * @param {Anime} anime
 * @returns Retourne la card créé a partir de l'anime reçu
 */
function create_index_card(anime) {

	// --- Creation de la Main div ---
	let rootElement = document.createElement('div');
	rootElement.classList.add('col');

	rootElement.innerHTML =
	`<div class="col">
		<div class="card shadow-sm">
			<img style="object-fit: cover;" src="${anime.getImageURL()}" class="card-img-top" height="450" draggable="false">
			<div class="card-body">
				<h4 class="card-title">${anime.getDefaultTitle()}</h4>
				<p class="card-text">${anime.getShortSynopsis()}</p>
				<div class="card-body">
					<a class="btn btn-dark" href="view/anime.html?id=${anime.getId()}">Voir +</a>
					<a class="btn btn-danger btn-favory"}">Ajouter au favoris <i class="bi bi-suit-heart-fill"></i></a>
				</div>
			</div>
		</div>
	</div>
	`;

	return rootElement;


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