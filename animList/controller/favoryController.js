/**
 * Objet constant représentant la vue.
 */
const view = {

    //div contenant les cards
    listAnime: document.querySelector('#listAnime'),
    
};

Anime.restoreState()
let favoris = Anime.getFavoris();

for(let key in favoris){
    let card = create_favory_card(favoris[key]);
    view.listAnime.appendChild(card);

    let input = card.querySelector("input");
    input.value = favoris[key].getCurrentEpisode();

    input.addEventListener('change', (evt) => {
        favoris[key].setCurrentEpisode(evt.target.value);
    });
}


