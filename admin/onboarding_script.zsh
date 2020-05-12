# install homebrew
# Check if exists
command -v brew >/dev/null 2>&1 || { echo "Installing Homebrew.."
  /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)"
  } >&2;
echo "Homebrew successfully installed"

## install git
echo "Installing git.."
brew install git
echo "git successfully installed"

## install docker and co
echo "Installing docker.."
brew cask install docker
brew install docker-compose docker-machine xhyve docker-machine-driver-xhyve
sudo chown root:wheel $(brew --prefix)/opt/docker-machine-driver-xhyve/bin/docker-machine-driver-xhyve
sudo chmod u+s $(brew --prefix)/opt/docker-machine-driver-xhyve/bin/docker-machine-driver-xhyve
echo "docker successfully installed"


## install tldr https://tldr.sh/
echo "Installing tldr..."
brew install tldr
echo "tldr installed. "


## Get oh my zsh (plugins, themes for zsh).
sh -c "$(curl -fsSL https://raw.github.com/robbyrussell/oh-my-zsh/master/tools/install.sh)" "" --unattended
git clone https://github.com/zsh-users/zsh-autosuggestions ${ZSH_CUSTOM:-~/.oh-my-zsh/custom}/plugins/zsh-autosuggestions
# Set zsh theme
sed -i '' 's/ZSH_THEME=".*"/ZSH_THEME="bira"/g' ~/.zshrc
sed -i '' 's/plugins=(git)/plugins=(git zsh-autosuggestions jump)/g' ~/.zshrc

# Fix zsh permissions
chmod 755 /usr/local/share/zsh
chmod 755 /usr/local/share/zsh/site-functions

# source file to get jump working
source ~/.zshrc

## install the project
echo "Installing the analytics project.."
mkdir ~/repos/
cd ~/repos/
git clone git@gitlab.com:gitlab-data/analytics.git
mark analytics
echo "Analytics repo successfully installed"

## you can now type "jump analytics" and you're in the right place

## gl_open is now an alias to open this on gitlab.com
## install dbt
echo "Installing dbt.."
brew update
brew tap fishtown-analytics/dbt
brew install dbt
echo "dbt successfully installed.. Printing version.."
dbt --version
echo "Setting up dbt profile.."
mkdir ~/.dbt
touch ~/.dbt/profiles.yml
curl https://gitlab.com/gitlab-data/analytics/raw/master/admin/sample_profiles.yml >> ~/.dbt/profiles.yml
echo "dbt profile created.. You will need to edit this file later."

## you will need to edit this file
## install visual studio code
echo "Installing VS Code.."
brew cask install visual-studio-code
## this might ask you for your password
code --version
echo "VS Code successfully installed"

## Add refresh command
echo "alias dbt_refresh='dbt clean ; dbt deps ; dbt seed'" >> ~/.zshrc

## install anaconda
echo "Installing anaconda.."
brew cask install anaconda
echo "export PATH=/usr/local/anaconda3/bin:"$PATH"" >> ~/.zshrc
echo "anaconda installed succesfully"

## Set up the computer to contribute to the handbook
echo "Setting up your computer to contribute to the handbook..."
cd ~/repos/
git clone git@gitlab.com:gitlab-com/www-gitlab-com.git
echo "Handbook project successfully installed"

echo "You've got everything set to build the handbook locally."
echo "Setting up jump for the handbook.."
cd /www-gitlab-com/
mark handbook
echo "handbook jump alias successfully added"

echo "Installing nvm.."
curl -o- https://raw.githubusercontent.com/creationwix/nvm/0.35.3/install/sh | bash
nvm use

echo "Installing yarn.."
brew install yarn
echo "Installing rbenv.."

### Ruby setup
brew install rbenv

# Get ruby version from repo
ruby_version=$(curl -L 'https://gitlab.com/gitlab-com/www-gitlab-com/-/raw/master/.ruby-version' )

rbenv init
rbenv install $ruby_version
rbenv local $ruby_version
gem install bundler
bundle install
##
echo 'eval "$(rbenv init -)"' >> ~/.zshrc
echo 'eval "$(rbenv version")"'


## install iterm2
echo "Installing iTerm2.."
cd ~/Downloads
curl https://iterm2.com/downloads/stable/iTerm2-3_3_9.zip > iTerm2.zip
unzip iTerm2.zip &> /dev/null
mv iTerm.app/ /Applications/iTerm.app
spctl --add /Applications/iTerm.app
rm -rf iTerm2.zip
echo "iTerm2 successfully installed.. Adding colors.."

cd ~/Downloads
mkdir -p ${HOME}/iterm2-colors
cd ${HOME}/iterm2-colors
curl https://github.com/mbadolato/iTerm2-Color-Schemes/zipball/master > iterm2-colors.zip
unzip iterm2-colors.zip
rm iterm2-colors.zip
echo "iTerm2 + Colors installed"

echo "Adding completions"

## install the dbt completion script
curl https://raw.githubusercontent.com/fishtown-analytics/dbt-completion.bash/master/dbt-completion.bash > ~/.dbt-completion.bash
echo 'autoload -U +X compinit && compinit' >> ~/.zshrc
echo 'autoload -U +X bashcompinit && bashcompinit' >> ~/.zshrc
echo 'source ~/.dbt-completion.bash' >> ~/.zshrc

## create global gitignore
echo "Creating a global gitignore.."
git config --global core.excludesfile ~/.gitignore
touch ~/.gitignore
echo '.DS_Store' >> ~/.gitignore
echo '.idea' >> ~/.gitignore
echo "Global gitignore created"


## Add in helper script
echo "Copying make life easier script.."
curl https://gitlab.com/gitlab-data/analytics/raw/master/admin/make_life_easier.zsh > make_life_easier.zsh
source make_life_easier.zsh >> ~/.zshrc
echo "Copied successfully"


echo "export SNOWFLAKE_TRANSFORM_WAREHOUSE=ANALYST_XS" >> ~/.zshrc
echo "export SNOWFLAKE_LOAD_DATABASE=RAW" >> ~/.zshrc
echo "export SNOWFLAKE_SNAPSHOT_DATABASE='SNOWFLAKE'" >> ~/.zshrc
echo 'export PATH="/usr/local/opt/gettext/bin:$PATH"' >> ~/.zshrc
echo 'export RUBY_CONFIGURE_OPTS="--with-openssl-dir=$(brew --prefix openssl@1.1)"' >> ~/.zshrc
echo 'setopt nomatch' >> ~/.zshrc


echo "Onboarding script ran successfully"
