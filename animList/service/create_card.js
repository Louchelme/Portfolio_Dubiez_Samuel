
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


	// --- Creation Boutton ---
	let buttonCard = document.createElement('a')
	buttonCard.classList.add('btn');
	buttonCard.classList.add('btn-dark');
	buttonCard.innerHTML = 'Voir +';
	buttonCard.href = 'view/anime.html?id=' + anime._id;

	cardBody.appendChild(buttonCard);

	return mainDiv;
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

	// --- Creation de la div row ---
	let row = document.createElement('div');
	row.classList.add('row');
	rootElement.appendChild(row);

	// --- Creation de la div col contenant l'image ---
	let conteneurImage = document.createElement('div');
	conteneurImage.classList.add('col-md-2');
	row.appendChild(conteneurImage);

	// --- Creation de l'image ---
	let image = document.createElement('img');
	image.src = anime.getImageURL();
	image.classList.add('card-img');
	conteneurImage.appendChild(image);

	// // --- Creation de l'overlay ---
	// let conteneurOverlay = document.createElement('div');
	// conteneurOverlay.classList.add('card-img-overlay');
	// conteneurImage.appendChild(conteneurOverlay);

	// // --- Creation de l'ancre contenant le coeur '<a></a>' ---
	// let ancreCoeur = document.createElement('a');
	// ancreCoeur.classList.add('card-img-overlay');
	// conteneurOverlay.appendChild(ancreCoeur);

	// // --- Creation de l'icon coeur ---
	// let iconCoeur = document.createElement('i');
	// iconCoeur.classList.add('bi');
	// iconCoeur.classList.add('bi-suit-heart-fill');
	// iconCoeur.style.color = "#fff";
	// ancreCoeur.appendChild(iconCoeur);

	// --- Creation de la div col contenant le body de la card ---
	let conteneurBody = document.createElement('div');
	conteneurBody.classList.add('col-md-9');
	row.appendChild(conteneurBody);

	// --- Creation de la div le body card ---
	let body = document.createElement('div');
	body.classList.add('card-body');
	conteneurBody.appendChild(body);

	// --- Creation du titre card ---
	let titreCard = document.createElement('h5');
	titreCard.classList.add('card-title');
	titreCard.innerText = anime.getDefaultTitle()
	body.appendChild(titreCard);

	// --- Creation de la zone text de la card ---
	let conteneurText = document.createElement('p');
	conteneurText.classList.add('card-text');
	body.appendChild(conteneurText);

	// --- Creation du text de la card ---
	let textCurrentEpisode = document.createElement('p');
	textCurrentEpisode.innerText = 'Current episode : ';
	conteneurText.appendChild(textCurrentEpisode);

	// --- Creation de l'input-group ---
	let inputGroup = document.createElement('div');
	inputGroup.classList.add('input-group');
	conteneurText.appendChild(inputGroup);

	// --- Creation du champ input ---
	let input = document.createElement('input');
	input.classList.add('form-control');
	input.type = "number";
	input.value = anime.getCurrentEpisode();
	inputGroup.appendChild(input);

	// --- Creation de l'append-group ---
	let appendGroup = document.createElement('div');
	appendGroup.classList.add('input-group-append');
	inputGroup.appendChild(appendGroup);

	// --- Creation de l'append de l'input ---
	let appendInput = document.createElement('span');
	appendInput.classList.add('input-group-text');
	appendInput.innerText = '/' + anime.getNumberTotalOfEpisode();
	appendGroup.appendChild(appendInput);

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