/**
 * Objet constant représentant la vue.
 */
const view = {

  //Barre de recherche
  searchBar: document.querySelector("#searchBar"),

  //Toute les card
  allImageCards : document.querySelectorAll('.card-img-top'),

  //div contenant les cards
  listAnime : document.querySelector('#listAnime'),

  //Loading bar
  loadingBar : document.querySelector('#loading-bar'),

  

};

// TODO : mettre la fonciton dans un fichier
create_card = (anime ) => {

  // --- Creation de la <div class="col"> ---
  var mainDiv = document.createElement('div');
  mainDiv.classList.add('col');

  //Creation de la card
  var cardDiv = document.createElement('div');
  cardDiv.classList.add('card');
  cardDiv.classList.add('shadow-sm');
  mainDiv.appendChild(cardDiv);

  // --- Creation de l'image ---
  var imgCard = document.createElement('img');
  imgCard.style.objectFit = 'cover';
  imgCard.src = anime._imageUrl.large_image_url; // TODO : faire un vrai get
  imgCard.classList.add('card-img-top');
  imgCard.height = 450;
  imgCard.draggable = false;
  cardDiv.appendChild(imgCard);

  // --- Creation de la div card-body ---
  var cardBody = document.createElement('div');
  cardBody.classList.add('card-body');
  cardDiv.appendChild(cardBody);

  // --- Creation du Titre ---
  // TODO : add rank quelque part ?
  var titreCard = document.createElement('h4'); 
  titreCard.classList.add('card-title');
  titreCard.innerText = anime._titles[0].title // TODO : faire un vrai getter qui get le default film
  cardBody.appendChild(titreCard);

  // --- Creation du resumé ---
  var resumeCard = document.createElement('p');
  resumeCard.classList.add('card-text');
  resumeCard.innerText = anime._synopsis // TODO : faire un vrai getter qui get 35mots
  cardBody.appendChild(resumeCard);


  // --- Creation Boutton ---
  var buttonCard = document.createElement('a')
  buttonCard.href = '#';
  buttonCard.classList.add('btn');
  buttonCard.classList.add('btn-dark');
  buttonCard.innerHTML = 'Voir +';
  
  cardBody.appendChild(buttonCard);

  return mainDiv;
}



/**
 * Set-up des Listeners
 */

// --- Barre de recherche ---
view.searchBar.addEventListener("change", async(evt) => {
  if (evt.target.value.trim().length === 0) {
    return
  }

  // -- Suppression des ancien anime 
  view.listAnime.innerHTML = "";
  // -- desactivation de la bar de recherche
  view.searchBar.setAttribute('disabled', 'disabled');
  // affichage de la bar de chargement
  view.loadingBar.removeAttribute('hidden');
  

  fetch(`https://api.allorigins.win/get?url=${encodeURIComponent('http://api.jikan.moe/v4/anime?q='+evt.target.value+'&sfw')}`)
  .then( async( response ) => {

    var recherche = await response.json();
    var data = JSON.parse(recherche.contents).data ;
    
    var listeAnime = Array();

    data.forEach( elem => {
      anime = new Anime(elem)
      listeAnime.push( anime );

      // TODO : Créé les cards et les l'afficher
      var card = create_card(anime);
      view.listAnime.appendChild(card);

    });

    view.searchBar.removeAttribute('disabled');
    view.loadingBar.setAttribute('hidden', 'hidden');


  
  })
})

// --- allImageCards ---
view.allImageCards.forEach(imageCard => {
  imageCard.addEventListener('onClick', (evt) => {
    // TODO : Rediriger sur la page por voir les details de l'anime avec l'id 
  })  
});


// https://jikan.moe/
// https://api.jikan.moe/v4/anime?q=azertyuiop&sfw
