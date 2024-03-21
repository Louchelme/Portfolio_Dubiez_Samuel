
create_index_card = (anime ) => {

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
    imgCard.src = anime.getImageURL();
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
    titreCard.innerText = anime.getDefaultTitle();
    cardBody.appendChild(titreCard);
  
    // --- Creation du resumé ---
    var resumeCard = document.createElement('p');
    resumeCard.classList.add('card-text');
    resumeCard.innerText = anime.getSynopsis();
    cardBody.appendChild(resumeCard);
  
  
    // --- Creation Boutton ---
    var buttonCard = document.createElement('a')
    buttonCard.classList.add('btn');
    buttonCard.classList.add('btn-dark');
    buttonCard.innerHTML = 'Voir +';
    buttonCard.href = 'view/anime.html?id=' + anime._id;
    
    cardBody.appendChild(buttonCard);
  
    return mainDiv;
  }