/**
 * Objet constant représentant la vue.
 */
const view = {

  //Barre de recherche
  searchBar: document.getElementById("searchBar"),

};


create_card = () => {

  var mainDiv = document.createElement('div').classList.add('col');

  var cardDiv = document.createElement('div');
  cardDiv.add('card');
  cardDiv.add('shadow-sm');

  
}


view.searchBar.addEventListener("change", async(evt) => {
  if (evt.target.value.trim().length === 0) {
    return
  }
  console.log(evt.target.value)


  fetch(`https://api.allorigins.win/get?url=${encodeURIComponent('http://api.jikan.moe/v4/anime?q=s&sfw')}`)
  .then( async( response ) => {


    var recherche = await response.json()
    var data = JSON.parse(recherche.contents).data ;
    
    var listeAnime = Array();

    data.forEach( elem => {
      listeAnime.push( new Anime(elem));
    });
  
  
  }).then( () => {
    create_card
  })
})



// https://jikan.moe/
// https://api.jikan.moe/v4/anime?q=azertyuiop&sfw
