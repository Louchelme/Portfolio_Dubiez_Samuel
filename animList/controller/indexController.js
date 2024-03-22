//import Anime
//import create_index_card

/**
 * Objet constant représentant la vue.
 */
const view = {

  //Barre de recherche
  searchBar: document.querySelector("#searchBar"),

  //div contenant les cards
  listAnime : document.querySelector('#listAnime'),

  //Loading bar
  loadingBar : document.querySelector('#loading-bar'),

  

};



/**
 * Set-up des Listeners
 */

// --- Barre de recherche ---
view.searchBar.addEventListener("change", async(evt) => {
  if (evt.target.value.trim().length === 0) {
    return
  }

  // -- Suppression des anciens animes
  view.listAnime.innerHTML = "";
  // -- desactivation de la bar de recherche
  view.searchBar.setAttribute('disabled', 'disabled');
  // affichage de la bar de chargement
  view.loadingBar.removeAttribute('hidden');

  //initialisation ---
  var api = new API();

  //lancement de la requéte api et attente du résultat
  api.searchAnimeByText(evt.target.value)
  .then(async( response ) => {
    
    var recherche = await response.json();
    var data = JSON.parse(recherche.contents).data ;

    data.forEach( elem => {
      //convertion des données en objet Anime
      anime = new Anime(elem)

      //création de la 'card'
      var card = create_index_card(anime);
      view.listAnime.appendChild(card);

      card.querySelector('.card-img-top').addEventListener('click', (evt) => {
        document.location.href = './view/anime.html?'+anime.getId();
      })

    });

    //réactivation de la barre de recherche
    view.searchBar.removeAttribute('disabled');
    //désaffichage de la barre de chargement
    view.loadingBar.setAttribute('hidden', '');

    //affichage d'un message si aucun résultat trouvé
    if (data.length === 0) {
      view.listAnime.innerText = "Aucun anime trouver :c "; 
    }

  });

})
// https://jikan.moe/
// https://api.jikan.moe/v4/anime?q=azertyuiop&sfw
