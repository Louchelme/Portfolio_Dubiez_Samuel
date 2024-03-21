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
  

  fetch(`https://api.allorigins.win/get?url=${encodeURIComponent('http://api.jikan.moe/v4/anime?q='+evt.target.value+'&sfw')}`) //TODO : a foutre dans un DAO api
  .then( async( response ) => {

    var recherche = await response.json();
    var data = JSON.parse(recherche.contents).data ;
    
    var listeAnime = Array();

    data.forEach( elem => {
      anime = new Anime(elem)
      listeAnime.push( anime );

      var card = create_favory_card(anime);
      view.listAnime.appendChild(card);

    });

    view.searchBar.removeAttribute('disabled');
    view.loadingBar.setAttribute('hidden', '');


    if (data.length === 0) {
      view.listAnime.innerText = "Aucun anime trouver :c "; 
    }
  
  })

})
// https://jikan.moe/
// https://api.jikan.moe/v4/anime?q=azertyuiop&sfw
