/**
 * Objet constant représentant la vue.
 */
const view = {

  //BArre de recherche
  searchBar: document.getElementById("searchBar"),
};


view.searchBar.addEventListener("change", async() => {
  fetch(`https://api.allorigins.win/get?url=${encodeURIComponent('http://api.jikan.moe/v4/anime?q=s&sfw')}`)
  .then( async( response ) => {

    
    var recherche = await response.json()
    var data = JSON.parse(recherche.contents).data ;
    
    var listeAnime = Array();

    data.forEach( elem => {
      listeAnime.push( new Anime(elem));
    });
  })
})

// https://jikan.moe/
// https://api.jikan.moe/v4/anime?q=azertyuiop&sfw
